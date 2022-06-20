from typing import TYPE_CHECKING, Set

if TYPE_CHECKING:
    from .db import ObjectDB


def transfer(
    src: "ObjectDB", dest: "ObjectDB", oids: Set["str"], jobs: int = None
) -> Set["str"]:
    src_exists = set(src.oids_exist(oids, jobs=jobs))
    src_missing = oids - src_exists

    dest_exists = set(dest.oids_exist(oids, jobs=jobs))
    dest_missing = oids - dest_exists

    missing = dest_missing & src_missing
    new = src_exists - dest_exists

    for oid in new:
        path = src.oid_to_path(oid)
        dest.add(path, src.fs, oid)

    if missing:
        raise Exception("missing objects", missing)
    return new
