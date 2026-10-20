"""Deterministic (non-LLM) parsers exposed to agents as tools. Stdlib only."""
import json
import re
import shlex
import xml.etree.ElementTree as ET


def _local(tag: str) -> str:
    return tag.split("}", 1)[-1]


def parse_oozie_workflow(xml_text: str) -> str:
    """Extract nodes and transitions from an Oozie workflow.xml."""
    try:
        root = ET.fromstring(xml_text.strip())
    except ET.ParseError as e:
        return f"ERROR: invalid XML: {e}"
    nodes = []
    for el in root:
        tag = _local(el.tag)
        node = {"node": tag, "name": el.attrib.get("name"), **{k: v for k, v in el.attrib.items() if k != "name"}}
        if tag == "start":
            node["to"] = el.attrib.get("to")
        elif tag == "action":
            for child in el:
                ctag = _local(child.tag)
                if ctag in ("ok", "error"):
                    node[ctag] = child.attrib.get("to")
                else:
                    node["action_type"] = ctag
                    node["config"] = {_local(c.tag): (c.text or "").strip() or c.attrib
                                      for c in child if (c.text or "").strip() or c.attrib}
        elif tag == "fork":
            node["paths"] = [p.attrib.get("start") for p in el]
        elif tag == "join":
            node["to"] = el.attrib.get("to")
        elif tag == "decision":
            node["cases"] = [c.attrib.get("to") for c in el.iter() if _local(c.tag) in ("case", "default")]
        nodes.append(node)
    return "OK: " + json.dumps(nodes, indent=1)


def parse_sqoop_command(text: str) -> str:
    """Parse one or more `sqoop import/export` command lines into flag dicts."""
    joined = re.sub(r"\\\s*\n", " ", text)
    cmds = []
    for line in joined.splitlines():
        if "sqoop" not in line:
            continue
        try:
            toks = shlex.split(line)
        except ValueError as e:
            return f"ERROR: cannot tokenise command: {e}"
        i = toks.index("sqoop") if "sqoop" in toks else 0
        toks = toks[i + 1:]
        if not toks:
            continue
        cmd = {"tool": toks[0], "flags": {}}
        j = 1
        while j < len(toks):
            if toks[j].startswith("-"):
                nxt = toks[j + 1] if j + 1 < len(toks) and not toks[j + 1].startswith("-") else "true"
                cmd["flags"][toks[j].lstrip("-")] = nxt
                j += 2 if nxt != "true" else 1
            else:
                j += 1
        cmds.append(cmd)
    if not cmds:
        return "ERROR: no sqoop command found"
    return "OK: " + json.dumps(cmds, indent=1)


_PY2_PATTERNS = {
    r"\bSQLContext\b": "SQLContext -> use SparkSession",
    r"\bHiveContext\b": "HiveContext -> SparkSession.builder.enableHiveSupport()",
    r"\bregisterTempTable\b": "registerTempTable -> createOrReplaceTempView",
    r"\.toDF\(\)\.registerTempTable": "registerTempTable -> createOrReplaceTempView",
    r"\bsc\.parallelize\b": "RDD API: consider DataFrame equivalent",
    r"\bprint\s+[^(\s]": "Python 2 print statement",
    r"\bxrange\b": "xrange -> range",
    r"\.iteritems\(\)": "iteritems -> items",
    r"\bunicode\(": "unicode -> str",
    r"\bsqlContext\.": "sqlContext -> spark",
    r"from pyspark\.sql import .*SQLContext": "import SQLContext -> SparkSession",
    r"\.jsonRDD\(|\.inferSchema\(": "removed/changed RDD-based schema APIs",
    r"to_date\(": "to_date format semantics changed in Spark 3 (timeParserPolicy)",
}


def scan_pyspark2(code: str) -> str:
    """List Spark 2 / Python 2 constructs that need migrating to Spark 3."""
    findings = []
    for lineno, line in enumerate(code.splitlines(), 1):
        for pat, msg in _PY2_PATTERNS.items():
            if re.search(pat, line):
                findings.append({"line": lineno, "issue": msg, "code": line.strip()})
    return "OK: " + json.dumps(findings or [{"issue": "none found"}], indent=1)
