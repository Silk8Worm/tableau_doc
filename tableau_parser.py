#!/usr/bin/env python3
"""
tableau_parser.py
============================
Parses a Tableau .twb or .xml workbook into a JSON file.
"""

import xml.etree.ElementTree as ET
import json, re, collections
from pathlib import Path


def _safe_filename(name: str) -> str:
    """Convert a dashboard/workbook name into a filesystem-safe stem."""
    if not name:
        return "tableau_workbook"
    s = re.sub(r"[^A-Za-z0-9]+", "_", name).strip('_')
    s = re.sub(r"_+", "_", s)
    return s or "tableau_workbook"

# ══════════════════════════════════════════════════════════════════════════════
# PARSER
# ══════════════════════════════════════════════════════════════════════════════

def build_global_caption(root):
    """Map every bare internal column name → human-readable caption."""
    gc = {}
    for col in root.iter("column"):
        raw = col.get("name", ""); cap = col.get("caption", "") or raw
        if not raw:
            continue
        bare = raw.strip("[]")
        if cap.startswith('"') and cap.endswith('"') and len(cap) > 2:
            cap = cap[1:-1]
        gc[bare] = cap
    for ci in root.iter("column-instance"):
        inst = ci.get("name", "").strip("[]")
        base = ci.get("column", "").strip("[]")
        if inst and base in gc:
            gc[inst] = gc[base]
    return gc


def build_formula_map(root):
    """Map every bare internal column name → its raw Tableau formula."""
    fm = {}
    for col in root.iter("column"):
        name = col.get("name", "").strip("[]")
        ce = col.find("calculation")
        if ce is not None:
            fm[name] = ce.get("formula", "")
    return fm


def clean_ref(ref: str) -> str:
    """
    Strip datasource prefix, ALL chained aggregation prefixes (pcto:, sum:, usr:, …),
    trailing suffixes (:nk, :ok, :qk…), and square brackets from a field reference.
    Also handles outer-quoted Measure Names member strings like '"[ds].[sum:Field:qk]"'.
    """
    s = ref.strip()
    # Strip outer quotes (Measure Names member values are double-quoted)
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1].strip()
    # Remove datasource prefix  [ds].  or  ds.[
    s = re.sub(r'^\[[^\[\]]*\]\.', '', s)
    s = re.sub(r'^[^\[\]]+\.\[', '[', s)
    s = s.strip("[]").strip()
    # Strip ALL chained aggregation prefixes
    while True:
        m = re.match(
            r'^(pcto:|sum:|usr:|attr:|none:|avg:|median:|count:|max:|min:|year:|month:|day:|week:)',
            s, re.I)
        if m:
            s = s[m.end():]
        else:
            break
    # Strip trailing derivation suffixes  :nk  :ok  :qk:2  etc.
    s = re.sub(r':(nk|ok|qk.*|pk)$', '', s, flags=re.I)
    return s.strip("[]").strip()


def resolve_field(ref: str, caption_map: dict) -> str:
    bare = clean_ref(ref)
    if bare in caption_map:
        return caption_map[bare].strip()
    b2 = f"[{bare}]"
    if b2 in caption_map:
        return caption_map[b2].strip()
    return bare


def expand_formula(formula: str, caption_map: dict) -> str:
    if not formula:
        return formula
    def _r(tok):
        bare = tok.group(0).strip("[]")
        cap = caption_map.get(bare)
        return f"({cap})" if cap and cap != bare else tok.group(0)
    return re.sub(r'\[[^\[\]\r\n]+\]', _r, formula)


MN_PATTERN = re.compile(r'\[:Measure Names\]|:Measure Names', re.I)


def is_measure_names_col(col_attr: str) -> bool:
    return bool(MN_PATTERN.search(col_attr))


def resolve_mn_members(filter_el, caption_map: dict) -> list:
    """
    Resolve the selected measure names from a Measure Names filter element.
    Each <groupfilter function="member"> has a member attr like:
        '"[odata.xxx].[sum:dollars:qk]"'
    We strip the outer quotes and datasource prefix, then resolve.
    """
    names = []
    for gf in filter_el.iter("groupfilter"):
        if gf.get("function") == "member":
            raw = gf.get("member", "")
            resolved = resolve_field(raw, caption_map)
            if resolved and resolved not in names:
                names.append(resolved)
    return names


def get_selected_values(filter_el) -> list:
    vals = []
    for gf in filter_el.iter("groupfilter"):
        if gf.get("function") == "member":
            raw = gf.get("member", "").replace("&quot;", '"').strip('"')
            if raw and raw not in vals:
                vals.append(raw)
    return vals


def parse_shelf_tokens(text: str) -> list:
    if not text:
        return []
    return [t for t in re.findall(
        r'\[[^\[\]]+\]\.\[[^\[\]]+\]|\[[^\[\]]+\]|[^\s\[\]]+', text
    ) if t.strip()]


def parse_workbook(xml_file: str) -> list:
    with open(xml_file, "rb") as f:
        content = f.read()
    root = ET.fromstring(content)

    gc = build_global_caption(root)
    fm = build_formula_map(root)

    # Parameters
    params_all = {}
    for col in root.iter("column"):
        if col.get("param-domain-type") or col.get("name", "").startswith("[Parameters]"):
            iname = col.get("name", ""); cap = col.get("caption", "") or iname
            params_all[iname] = {
                "name": cap.strip(),
                "datatype": col.get("datatype", ""),
                "value": col.get("value", "") or col.get("default-value", ""),
            }

    # ── Parse worksheets ──────────────────────────────────────────────────
    ws_data = collections.OrderedDict()

    for ws in root.iter("worksheet"):
        ws_name = ws.get("name", "")
        rec = dict(name=ws_name, filters=[], calculated_fields=[], visualization=[])

        # Per-worksheet local caption / formula overrides
        lc = dict(gc)
        lf = {}
        for dep in ws.iter("datasource-dependencies"):
            for col in dep.findall("column"):
                raw = col.get("name", ""); cap = col.get("caption", "") or raw
                bare = raw.strip("[]")
                if cap.startswith('"') and cap.endswith('"') and len(cap) > 2:
                    cap = cap[1:-1]
                lc[bare] = cap
                ce = col.find("calculation")
                if ce is not None:
                    lf[bare] = ce.get("formula", "")
            for ci in dep.findall("column-instance"):
                inst = ci.get("name", "").strip("[]")
                base = ci.get("column", "").strip("[]")
                if inst and base in lc:
                    lc[inst] = lc[base]

        def lr(r): return resolve_field(r, lc)
        def le(f): return expand_formula(f, lc)

        # Calculated fields
        cf_seen = set()
        for dep in ws.iter("datasource-dependencies"):
            for col in dep.findall("column"):
                bare = col.get("name", "").strip("[]")
                cap = col.get("caption", "") or bare
                if cap.startswith('"') and cap.endswith('"') and len(cap) > 2:
                    cap = cap[1:-1]
                ce = col.find("calculation")
                if ce is None or not ce.get("formula", "") or bare in cf_seen:
                    continue
                cf_seen.add(bare)
                formula = ce.get("formula", "")
                usages = []
                for flt in ws.iter("filter"):
                    if bare in (flt.get("column", "") or ""):
                        usages.append("Filter"); break
                rec["calculated_fields"].append({
                    "internal_name": bare, "name": cap.strip(),
                    "formula": formula, "expanded_formula": le(formula),
                    "usages": list(set(usages)), "is_raw_field": False,
                    "datasource": dep.get("datasource", ""),
                })

        # Filters
        f_seen = set()
        for flt in ws.iter("filter"):
            col_attr = flt.get("column", "") or flt.get("field", "")
            if not col_attr or col_attr in f_seen:
                continue
            f_seen.add(col_attr)
            ftype = flt.get("class", "") or flt.get("type", "")
            calc_el = flt.find("calculation")
            formula = calc_el.get("formula", "") if calc_el is not None else ""

            if is_measure_names_col(col_attr):
                sel_vals = resolve_mn_members(flt, lc)
                rec["filters"].append({
                    "field_name": "Measure Names", "raw_field": col_attr,
                    "filter_type": ftype, "formula": formula,
                    "expanded_formula": le(formula) if formula else "",
                    "selected_values": sel_vals,
                    "shown_on_dashboard": False, "is_measure_names": True,
                })
            else:
                rec["filters"].append({
                    "field_name": lr(col_attr), "raw_field": col_attr,
                    "filter_type": ftype, "formula": formula,
                    "expanded_formula": le(formula) if formula else "",
                    "selected_values": get_selected_values(flt),
                    "shown_on_dashboard": False, "is_measure_names": False,
                })

        # Visualization
        mn_measures = next(
            (f["selected_values"] for f in rec["filters"] if f.get("is_measure_names")), []
        )
        viz_seen = set()

        def add_viz(field_ref: str, shelf: str):
            bare_chk = clean_ref(field_ref)
            is_mn = bare_chk in (":Measure Names", "Measure Names") or \
                    MN_PATTERN.search(field_ref) is not None
            is_mv = bare_chk in ("Multiple Values", "Measure Values") or \
                    "Multiple Values" in field_ref

            if is_mn:
                key = ("Measure Names", shelf)
                if key not in viz_seen:
                    viz_seen.add(key)
                    rec["visualization"].append({
                        "field_name": "Measure Names", "shelf": shelf,
                        "formula": "", "expanded_formula": "",
                        "underlying_measures": mn_measures,
                        "notes": "Underlying measures: " + "; ".join(mn_measures) if mn_measures else "",
                    })
                return

            if is_mv:
                key = ("Measure Values", shelf)
                if key not in viz_seen:
                    viz_seen.add(key)
                    rec["visualization"].append({
                        "field_name": "Measure Values", "shelf": shelf,
                        "formula": "", "expanded_formula": "",
                        "underlying_measures": mn_measures,
                        "notes": "Aggregated values for: " + "; ".join(mn_measures) if mn_measures else "",
                    })
                return

            human = lr(field_ref)
            if not human:
                return
            key = (human, shelf)
            if key in viz_seen:
                return
            viz_seen.add(key)
            bare = clean_ref(field_ref)
            formula = lf.get(bare, "") or fm.get(bare, "")
            rec["visualization"].append({
                "field_name": human, "shelf": shelf,
                "formula": formula, "expanded_formula": le(formula),
                "underlying_measures": [], "notes": "",
            })

        for el in ws.iter("rows"):
            for token in parse_shelf_tokens(el.text or ""):
                add_viz(token, "Row")
        for el in ws.iter("cols"):
            for token in parse_shelf_tokens(el.text or ""):
                add_viz(token, "Column")
        for pane in ws.iter("pane"):
            for enc_el in pane.iter("encodings"):
                for child in enc_el:
                    col_ref = child.get("column", "")
                    if col_ref:
                        add_viz(col_ref, child.tag.capitalize())
        for marks_el in ws.iter("marks"):
            for enc in marks_el.iter("encoding"):
                col_ref  = enc.get("column", "") or enc.get("field", "")
                enc_type = enc.get("type", "") or enc.get("class", "")
                if col_ref:
                    add_viz(col_ref, enc_type.capitalize() if enc_type else "Mark")

        ws_data[ws_name] = rec

    # ── Parse dashboards ──────────────────────────────────────────────────
    dashboards_out = []

    for db in root.iter("dashboard"):
        db_name = db.get("name", "")
        ws_list = []; shown = set()

        for zone in db.iter("zone"):
            wname  = zone.get("name", "") or zone.get("worksheet", "")
            z_type = zone.get("type", "")
            if wname and wname in ws_data and wname not in ws_list:
                ws_list.append(wname)
            if "filter" in z_type.lower() or "param" in z_type.lower():
                pf = zone.get("param", "") or zone.get("name", "")
                if pf:
                    shown.add(pf)

        for wname in ws_list:
            for flt in ws_data[wname]["filters"]:
                if flt.get("raw_field", "") in shown or flt.get("field_name", "") in shown:
                    flt["shown_on_dashboard"] = True

        db_filters = []; dfs = set()
        for flt in db.iter("filter"):
            col_attr = flt.get("column", "") or flt.get("field", "")
            if not col_attr or col_attr in dfs:
                continue
            dfs.add(col_attr)
            ftype   = flt.get("class", "") or flt.get("type", "")
            calc_el = flt.find("calculation")
            formula = calc_el.get("formula", "") if calc_el is not None else ""
            if is_measure_names_col(col_attr):
                db_filters.append({
                    "field_name": "Measure Names", "filter_type": ftype,
                    "formula": formula, "expanded_formula": expand_formula(formula, gc),
                    "selected_values": resolve_mn_members(flt, gc),
                    "shown_on_dashboard": False, "is_measure_names": True,
                })
            else:
                db_filters.append({
                    "field_name": resolve_field(col_attr, gc),
                    "filter_type": ftype, "formula": formula,
                    "expanded_formula": expand_formula(formula, gc),
                    "selected_values": get_selected_values(flt),
                    "shown_on_dashboard": resolve_field(col_attr, gc) in shown or col_attr in shown,
                    "is_measure_names": False,
                })

        db_params = list({p["name"]: p for p in params_all.values()}.values())
        dashboards_out.append({
            "name": db_name, "parameters": db_params,
            "filters": db_filters,
            "worksheets": [dict(ws_data[w]) for w in ws_list if w in ws_data],
        })

    return dashboards_out


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent

    # 1) Auto-select the first supported workbook (.twb or .xml) in the same folder.
    candidates = sorted(
        list(script_dir.glob("*.twb")) +
        list(script_dir.glob("*.xml"))
    )

    if not candidates:
        raise RuntimeError(
            "No supported workbook found in the script directory. "
            "Place a .twb or .xml Tableau workbook file next to this script."
        )

    xml_file = candidates[0]

    # 2) Validate the file extension.
    supported_extensions = {".twb", ".xml"}
    if xml_file.suffix.lower() not in supported_extensions:
        raise ValueError(
            f"Unsupported file type '{xml_file.suffix}'. "
            "Only .twb and .xml files are supported."
        )

    print(f"Parsing {xml_file} ...")
    dashboards = parse_workbook(str(xml_file))

    # 3) Name outputs using the workbook name.
    stem = _safe_filename(xml_file.stem)
    json_out = str(script_dir / f"{stem}.json")

    # Save JSON
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(dashboards, f, ensure_ascii=False, indent=2)
    print(f"JSON saved: {json_out}")
