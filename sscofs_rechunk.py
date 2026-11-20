# %% [markdown]
# Virtual SSCOFS subchunks (Icechunk 2.3.1 and Zarr 3).
# Split uncompressed science chunks into single-time, single-layer byte ranges.
# Horizontal partitions remain unchanged; no science arrays are materialized.
# Small native coordinate chunks are copied; virtual chunks still point to NOAA.
# Source files must stay at stable URLs. The reference iterator does not expose
# checksum guards, so those guards cannot be carried into the new references.
# Run as Python or execute the notebook-style cells. The final cell writes S3.

# %%
import asyncio
import copy
from concurrent.futures import ThreadPoolExecutor
from itertools import product
import json
from math import prod

import icechunk
import numpy as np
import zarr
from zarr.core.buffer import default_buffer_prototype


# %%
bucket = "nextgen-dmac-cloud-ingest-cmorse-dev"
source_prefix = "sscofs/sscofs-icechunk-best"
destination_prefix = "sscofs/sscofs-icechunk-best-virtual-subchunks"
region = "us-east-1"
noaa_prefix = "s3://noaa-nos-ofs-pds/"
buffer_prototype = default_buffer_prototype()


# %%
def run_async(factory):
    """Support both Python and a notebook kernel with an active event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())
    with ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(lambda: asyncio.run(factory())).result()


def encode_metadata(metadata):
    return buffer_prototype.buffer.from_bytes(json.dumps(metadata).encode())


def chunk_key(name, metadata, index):
    encoding = metadata["chunk_key_encoding"]
    if encoding["name"] != "default":
        raise ValueError(f"{name}: only default Zarr v3 chunk keys are supported")
    separator = encoding.get("configuration", {}).get("separator", "/")
    return f"{name}/" + separator.join(("c", *(str(i) for i in index)))


def subchunk_layout(name, metadata):
    """Only split dimensions that produce contiguous C-order byte ranges."""
    old = tuple(metadata["chunk_grid"]["configuration"]["chunk_shape"])
    dims = metadata.get("dimension_names") or (None,) * len(old)
    new = tuple(1 if d in ("time", "siglay", "siglev") else c
                for d, c in zip(dims, old)) if len(old) > 1 else old
    if new != old:
        codecs = metadata["codecs"]
        if len(codecs) != 1 or codecs[0]["name"] != "bytes":
            raise ValueError(f"{name}: cannot split compressed/transformed chunks")
        if metadata.get("storage_transformers"):
            raise ValueError(f"{name}: storage transformers are unsupported")
        last_split = max(i for i in range(len(old)) if old[i] != new[i])
        if any(c != 1 for c in new[:last_split]):
            raise ValueError(f"{name}: subchunks require noncontiguous reads")
        if any(a % b for a, b in zip(old, new)):
            raise ValueError(f"{name}: subchunks must divide the old chunk shape")
        if np.dtype(metadata["data_type"]).kind not in "biufc":
            raise ValueError(f"{name}: expected a fixed-size numeric dtype")
    return old, new


def split_reference(name, metadata, old, new, index, location, offset, length):
    """Derive virtual subranges without fetching their payloads."""
    if old == new:
        return [icechunk.VirtualChunkSpec(list(index), location, offset, length)]
    itemsize = np.dtype(metadata["data_type"]).itemsize
    if length != prod(old) * itemsize:
        raise ValueError(f"{name}: reference length differs from raw chunk size")
    factors = tuple(a // b for a, b in zip(old, new))
    strides = tuple(prod(old[i + 1:]) for i in range(len(old)))
    specs = []
    for part in product(*(range(factor) for factor in factors)):
        target = tuple(int(i) * factor + p for i, factor, p in zip(index, factors, part))
        # HDF5 edge chunks may be padded: skip out-of-array layers/times.
        if any(i * c >= s for i, c, s in zip(target, new, metadata["shape"])):
            continue
        displacement = sum(p * c * stride for p, c, stride in zip(part, new, strides))
        specs.append(icechunk.VirtualChunkSpec(
            list(target), location, offset + displacement * itemsize,
            prod(new) * itemsize,
        ))
    return specs


async def plan_subchunks(source_session):
    """Validate all references before creating the destination repository."""
    source = await zarr.api.asynchronous.open_group(
        source_session.store, mode="r", use_consolidated=False
    )
    if [name async for name in source.group_keys()]:
        raise ValueError("This SSCOFS workflow expects root arrays, without subgroups")
    raw = await source_session.store.get("zarr.json", buffer_prototype)
    root_metadata = json.loads(raw.to_bytes())
    root_metadata.pop("consolidated_metadata", None)
    plans = []
    arrays = [pair async for pair in source.arrays()]
    for name, array in sorted(arrays):
        raw = await source_session.store.get(f"{name}/zarr.json", buffer_prototype)
        metadata = json.loads(raw.to_bytes())
        if metadata["chunk_grid"]["name"] != "regular":
            raise ValueError(f"{name}: expected a regular chunk grid")
        chunk_key(name, metadata, (0,) * array.ndim)
        old, new = subchunk_layout(name, metadata)
        virtual, native, inline = [], [], []
        async for indices, kinds, paths, offsets, lengths, inlined in source_session.store.array_chunk_iterator(name):
            for row, coordinate in enumerate(indices):
                index = tuple(map(int, coordinate))
                kind = int(kinds[row])
                if kind == 2:
                    virtual.extend(split_reference(
                        name, metadata, old, new, index, paths[row],
                        int(offsets[row]), int(lengths[row]),
                    ))
                elif old != new:
                    raise ValueError(f"{name}: splitting requires virtual chunks")
                elif kind == 1:
                    native.append(chunk_key(name, metadata, index))
                elif kind == 3:
                    inline.append((chunk_key(name, metadata, index), inlined[row]))
                else:
                    raise ValueError(f"{name}: unsupported chunk kind {kind}")
        updated = copy.deepcopy(metadata)
        updated["chunk_grid"]["configuration"]["chunk_shape"] = list(new)
        plans.append(dict(name=name, metadata=updated, old=old, new=new,
                          virtual=virtual, native=native, inline=inline))
        print(f"{name}: {old} -> {new}; {len(virtual)} virtual references")
    return root_metadata, plans


async def write_plan(source_session, destination_session, root_metadata, plans):
    await destination_session.store.set("zarr.json", encode_metadata(root_metadata))
    for plan in plans:
        name = plan["name"]
        await destination_session.store.set(f"{name}/zarr.json", encode_metadata(plan["metadata"]))
        refs = plan["virtual"]
        for start in range(0, len(refs), 2000):
            failed = await destination_session.store.set_virtual_refs_async(
                f"/{name}", refs[start:start + 2000], validate_containers=True
            )
            if failed:
                raise ValueError(f"{name}: unauthorized virtual references: {failed}")
        for key in plan["native"]:
            payload = await source_session.store.get(key, buffer_prototype)
            if payload is None:
                raise ValueError(f"Missing native source chunk: {key}")
            await destination_session.store.set(key, payload)
        for key, payload in plan["inline"]:
            await destination_session.store.set(key, buffer_prototype.buffer.from_bytes(payload))


async def validate_samples(source_session, destination_session, plans):
    """Read bounded samples at the start, chunk boundaries, and padded edges."""
    source = await zarr.api.asynchronous.open_group(
        source_session.store, mode="r", use_consolidated=False
    )
    result = await zarr.api.asynchronous.open_group(
        destination_session.store, mode="r", use_consolidated=False
    )
    assert {n async for n in source.array_keys()} == {n async for n in result.array_keys()}
    for plan in plans:
        name = plan["name"]
        a, b = await source.getitem(name), await result.getitem(name)
        assert b.shape == a.shape and b.chunks == plan["new"], name
        assert dict(a.attrs) == dict(b.attrs), name
        if plan["old"] == plan["new"]:
            continue
        for position in (0, 0.5, 1):
            selection = []
            for axis, (dim, size) in enumerate(zip(a.metadata.dimension_names, a.shape)):
                index = min(a.chunks[axis], size - 1) if position == 0.5 else int(position * (size - 1))
                selection.append(slice(max(0, index - 2), min(size, index + 3))
                                 if dim in ("node", "nele") else index)
            np.testing.assert_array_equal(
                await a.getitem(tuple(selection)), await b.getitem(tuple(selection)),
                err_msg=name,
            )
        print(f"Validated {name}: first, boundary, and final time/layer samples")


# %%
async def build_virtual_subchunks():
    if source_prefix.strip("/") == destination_prefix.strip("/"):
        raise ValueError("Source and destination repository prefixes must differ")
    creds = icechunk.containers_credentials(
        {noaa_prefix: icechunk.s3_credentials(anonymous=True)}
    )
    source_storage = icechunk.s3_storage(
        bucket=bucket, prefix=source_prefix, region=region, from_env=True
    )
    repo = icechunk.Repository.open(source_storage, authorize_virtual_chunk_access=creds)
    source_session = repo.readonly_session("main")
    print(f"Source snapshot: {source_session.snapshot_id}")
    root_metadata, plans = await plan_subchunks(source_session)
    destination_storage = icechunk.s3_storage(
        bucket=bucket, prefix=destination_prefix, region=region, from_env=True
    )
    # Fails if the destination already exists; never overwrite or fall back.
    mapping_repo = icechunk.Repository.create(
        destination_storage, config=repo.config, authorize_virtual_chunk_access=creds
    )
    mapping_repo.save_config()
    writer = mapping_repo.writable_session("main")
    await write_plan(source_session, writer, root_metadata, plans)
    snapshot_id = writer.commit("Split raw virtual chunks into single-layer byte ranges")
    print(f"Committed snapshot: {snapshot_id}")

    # Validate from the public consumer endpoint in a fresh read-only session.
    base = f"https://{bucket}.s3.{region}.amazonaws.com/"
    reader = icechunk.Repository.open(
        icechunk.http_storage(base + destination_prefix),
        authorize_virtual_chunk_access=creds,
    )
    readonly = reader.readonly_session(snapshot_id=snapshot_id)
    await validate_samples(source_session, readonly, plans)
    for plan in plans:
        if plan["old"] != plan["new"]:
            count = 0
            async for _, kinds, *_ in readonly.store.array_chunk_iterator(plan["name"]):
                assert np.all(kinds == 2), f"{plan['name']}: found nonvirtual science chunks"
                count += len(kinds)
            assert count == len(plan["virtual"]), plan["name"]
    print("Verified subchunk data and virtual reference counts.")
    for label, prefix in (("Original", source_prefix), ("Virtual subchunks", destination_prefix)):
        print(f"{label}: {base}{prefix}/")
    return snapshot_id


# %%
# Executing this final cell writes the new repository to S3.
if __name__ == "__main__":
    snapshot_id = run_async(build_virtual_subchunks)
