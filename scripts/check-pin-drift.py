#!/usr/bin/env python3
"""Cross-POM pin-drift gate (over-engineering audit 2026-07-25; CHANGELOG v1.15).

The four service POMs are standalone by design (ADR-17 §3.5 — no shared parent/BOM to
hold pins), so version alignment is a convention, not a build fact. This gate makes it
a build fact: any (groupId, artifactId) declared with an explicit version in TWO OR MORE
POMs must resolve to a single version. Parent coordinates count. BOM-managed dependencies
(no <version> tag) are naturally out of scope.

Renovate (renovate.json) keeps pins aligned on upgrade; this catches manual one-POM edits.

Usage: check-pin-drift.py <pom.xml> [<pom.xml> ...]   (exit 1 on drift)
"""
import re
import sys
import xml.etree.ElementTree as ET

# Deliberate divergences go here as "groupId:artifactId" with a reason comment. Empty today.
ALLOW: set[str] = set()

# groupIds whose artifacts must ALL resolve to one version repo-wide, even when the POMs
# declare disjoint artifactIds (detection pins resilience4j-spring-boot4/-reactor, triage
# pins the six core modules — per-coordinate checking alone cannot see that drift).
GROUP_ALIGN: set[str] = {"io.github.resilience4j"}


def local(tag: str) -> str:
    return tag.split("}")[-1]


def child_text(elem, name):
    for c in elem:
        if local(c.tag) == name:
            return (c.text or "").strip()
    return None


def collect(pom_path):
    """Return {coordinate: resolved_version} for explicitly versioned declarations."""
    root = ET.parse(pom_path).getroot()
    props = {}
    for elem in root:
        if local(elem.tag) == "properties":
            for p in elem:
                props[local(p.tag)] = (p.text or "").strip()

    def resolve(version):
        m = re.fullmatch(r"\$\{([^}]+)\}", version or "")
        return props.get(m.group(1), version) if m else version

    pins = {}
    for parent in (e for e in root if local(e.tag) == "parent"):
        coord = f"{child_text(parent, 'groupId')}:{child_text(parent, 'artifactId')}"
        pins[coord] = child_text(parent, "version")
    for dep in root.iter():
        if local(dep.tag) != "dependency":
            continue
        version = child_text(dep, "version")
        if version:  # unversioned = BOM/parent-managed, out of scope
            coord = f"{child_text(dep, 'groupId')}:{child_text(dep, 'artifactId')}"
            pins[coord] = resolve(version)
    return pins


def main(pom_paths):
    declared = {}  # coord -> {pom: version}
    for path in pom_paths:
        for coord, version in collect(path).items():
            declared.setdefault(coord, {})[path] = version

    drift = {
        coord: by_pom
        for coord, by_pom in declared.items()
        if len(by_pom) >= 2 and len(set(by_pom.values())) > 1 and coord not in ALLOW
    }
    shared = {c: v for c, v in declared.items() if len(v) >= 2}

    group_drift = {}
    for group in GROUP_ALIGN:
        versions = {}  # version -> [(pom, coord)]
        for coord, by_pom in declared.items():
            if coord.split(":")[0] == group:
                for pom, version in by_pom.items():
                    versions.setdefault(version, []).append((pom, coord))
        if len(versions) > 1:
            group_drift[group] = versions

    if drift or group_drift:
        if drift:
            print("PIN DRIFT — same coordinate, different versions across standalone POMs:")
            for coord, by_pom in sorted(drift.items()):
                print(f"  {coord}")
                for pom, version in sorted(by_pom.items()):
                    print(f"    {pom}: {version}")
        if group_drift:
            print("GROUP DRIFT — GROUP_ALIGN groupId resolving to multiple versions:")
            for group, versions in sorted(group_drift.items()):
                print(f"  {group}")
                for version, sites in sorted(versions.items()):
                    for pom, coord in sorted(sites):
                        print(f"    {version}  {pom} ({coord.split(':')[1]})")
        print("Align the versions (or allowlist in this script with a reason).")
        return 1

    print(f"pin-drift: OK — {len(shared)} shared coordinate(s) aligned across {len(pom_paths)} POMs:")
    for coord, by_pom in sorted(shared.items()):
        print(f"  {coord} = {next(iter(by_pom.values()))} ({len(by_pom)} POMs)")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: check-pin-drift.py <pom.xml> [<pom.xml> ...]")
    sys.exit(main(sys.argv[1:]))
