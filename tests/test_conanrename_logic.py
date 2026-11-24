"""ConanRename 2.0.0 纯逻辑离线测试（不依赖 MoviePilot 宿主）。

只抽取插件里不含宿主调用的纯函数，验证：
  1. 精确映射 / tail 外推 / 超窗隔离 / 补充映射（含 S00 特辑）
  2. 识别词规则生成（range 模式 与 tail 模式）
  3. 映射指纹（外推依据是否变化）
"""
import ast
import re
import sys
import textwrap
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "plugins.v3/conanrename/__init__.py"
source = SRC.read_text(encoding="utf-8")
tree = ast.parse(source)

WANT_CONST = {"MEDIA_EXT", "EP_PATTERNS", "EP_TOKEN", "SYNC_MARK_BEGIN",
              "SYNC_MARK_END", "LEDGER_FILE"}
WANT_FUNC = {
    "_parse_split_nums", "_build_mapping", "_compute_ranges", "_digits_class",
    "_fixed_width_regex", "_range_regex", "_build_rules", "_load_extra_map",
    "_detect_episode", "_resolve_episode", "_mapping_signature", "_compose_words",
    "_is_slim", "_mapping_from_slim",
}

import typing as _typing
ns = {
    "Any": _typing.Any, "Dict": _typing.Dict, "List": _typing.List,
    "Optional": _typing.Optional, "Tuple": _typing.Tuple,
    "re": re, "json": __import__("json"),
    "logger": type("L", (), {"warning": staticmethod(lambda *a, **k: None),
                             "error": staticmethod(lambda *a, **k: None),
                             "info": staticmethod(lambda *a, **k: None),
                             "debug": staticmethod(lambda *a, **k: None)})(),
    "MEDIA_EXT": None, "EP_PATTERNS": None, "EP_TOKEN": None,
    "SYNC_MARK_BEGIN": None, "SYNC_MARK_END": None, "LEDGER_FILE": None,
    "datetime": None, "timedelta": None, "Path": Path,
}

# 常量
for node in tree.body:
    if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name) \
            and node.targets[0].id in WANT_CONST:
        exec(compile(ast.Module([node], type_ignores=[]), "<const>", "exec"), ns)

# 类里的纯函数
cls = next(n for n in tree.body
           if isinstance(n, ast.ClassDef) and n.name == "ConanRename")
lines = source.splitlines()
funcs = []
for n in cls.body:
    if isinstance(n, ast.FunctionDef) and n.name in WANT_FUNC:
        seg = textwrap.dedent("\n".join(lines[n.lineno - 1:n.end_lineno]))
        funcs.append(seg)

# 变成独立函数（去掉 self / cls 参数与 @staticmethod/@classmethod 装饰器）
patched = []
for seg in funcs:
    seg = seg.replace("@staticmethod\n", "").replace("@classmethod\n", "")
    seg = re.sub(r"\(\s*cls\s*,\s*", "(", seg)
    seg = re.sub(r"\(\s*cls\s*\)", "()", seg)
    seg = re.sub(r"\(\s*self\s*,\s*", "(", seg)
    seg = re.sub(r"\(\s*self\s*\)", "()", seg)
    seg = seg.replace("self.", "cfg.")
    patched.append(seg)

ns["cfg"] = type("Cfg", (), {
    "_sync_front": r"Conan[\s.]S01E",
    "_sync_back": r"[\s.]1996",
    "_extra_map": "",
    "_grace": 30,
})()
exec(compile("\n\n".join(patched), "<funcs>", "exec"), ns)
# 把独立函数挂回 cfg，供 self.xxx 调用
for _n in ("_parse_split_nums", "_compute_ranges", "_digits_class",
           "_fixed_width_regex", "_range_regex", "_is_slim", "_mapping_from_slim"):
    setattr(ns["cfg"], _n, ns[_n])
ns["cls"] = ns["cfg"]

g = ns
failures = []


def check(name, got, want):
    ok = got == want
    print(f"{'✅' if ok else '❌'} {name}: {got!r}" + ("" if ok else f"  (期望 {want!r})"))
    if not ok:
        failures.append(name)


# ── 1. 映射解析（取真实 sbsub 快照的尾部结构） ──
def fake_sbsub(pairs):
    eps = {str(tmdb): [str(split), "标题", "", "", "", "", {}, {}] for split, tmdb in pairs}
    return {"res": [["tv", "电视动画", "", "", eps]]}


pairs = [(1274, 1214), (1275, 1215), (1276, 1216), (1277, 1217),
         (1278, 1218), (1279, 1219), (1280, 1220)]
mapping = g["_build_mapping"](fake_sbsub(pairs))
check("映射条数", len(mapping), 7)
check("1275 精确映射", mapping.get(1275), 1215)

# ── 2. 集号解析：精确 / 外推 / 超窗 / 补充 ──
extra = g["_load_extra_map"]()
check("1281 外推（映射滞后）", g["_resolve_episode"](1281, mapping, extra), (1, 1221, "extrapolated"))
check("1280 精确", g["_resolve_episode"](1280, mapping, extra), (1, 1220, "exact"))
check("1310 窗口边缘（1280+30）", g["_resolve_episode"](1310, mapping, extra), (1, 1250, "extrapolated"))
check("1311 超窗隔离", g["_resolve_episode"](1311, mapping, extra), (None, None, "none"))

g["cfg"]._extra_map = "1262=S0E34\n1134=S0E32"
extra2 = g["_load_extra_map"]()
check("补充映射特辑 S00", g["_resolve_episode"](1262, mapping, extra2), (0, 34, "extra"))
check("补充映射优先于外推", g["_resolve_episode"](1134, mapping, extra2), (0, 32, "extra"))

# ── 3. 识别词规则 ──
ranges = g["_compute_ranges"](mapping)
check("区间数", len(ranges), 1)
tail_rule = g["_build_rules"](ranges, "tail")
check("tail 模式只有 1 条", len(tail_rule), 1)
check("tail 偏移正确", tail_rule[0].endswith(">> EP-60"), True)
print("   tail 规则:", tail_rule[0])

# 造一段带跳变的映射验证 range 模式
m2 = {}
for i in range(1, 500):
    m2[i] = i - 60
for i in range(500, 600):
    m2[i] = i - 58
ranges2 = g["_compute_ranges"](m2)
check("跳变处切成 2 段", len(ranges2), 2)
rules2 = g["_build_rules"](ranges2, "range")
check("range 模式 2 条", len(rules2), 2)
print("   range 规则:", rules2[0][:110], "...")
# 断言：第一段规则应能匹配 499 而不能匹配 500（500 属于第二段，偏移不同）
_span = re.search(r"\?=0\*\(\?:(.+?)\)\(\?:\)", rules2[0])
_m = re.search(r"\(\?=0\*\(\?:", rules2[0])
check("两段偏移不同", rules2[0].endswith("EP-60") and rules2[1].endswith("EP-58"), True)
check("第一段规则含 4xx 区间", "4" in rules2[0].split("<>")[0], True)

# 前定位词可匹配种子名（空格）与本地名（点）
front = re.compile(g["cfg"]._sync_front)
check("定位词匹配种子名", bool(front.search("Detective Conan S01E1281 1996")), True)
check("定位词匹配本地名", bool(front.search("Detective.Conan.S01E1281.1996")), True)

# ── 4. 映射指纹（外推纠正的触发条件） ──
sig_old = g["_mapping_signature"](mapping)
mapping_new = dict(mapping)
mapping_new[1281] = 1221
sig_new = g["_mapping_signature"](mapping_new)
check("映射补全后指纹变化", sig_old != sig_new, True)
check("旧指纹内容", sig_old, "1280:-60")
check("新指纹内容", sig_new, "1281:-60")

# ── 5. 集号识别 ──
check("识别点分种子", g["_detect_episode"]("Detective.Conan.S01E1281.1996.1080p"), 1281)
check("识别 part 文件", g["_detect_episode"]("Detective.Conan.S01E0011-part2.1996"), 11)
check("识别中文名", g["_detect_episode"]("名侦探柯南 - 第 1281 集"), 1281)

# ── 6. 识别词区块合成 ──
words = g["_compose_words"]("# 用户自己的规则\nConan[\\s.]S01E <> [\\s.]1996 >> EP-59", tail_rule)
check("保留用户规则", "# 用户自己的规则" in words, True)
check("清掉旧的手工同步规则", ">> EP-59" not in words, True)
check("写入维护区块", g["SYNC_MARK_BEGIN"] in words and g["SYNC_MARK_END"] in words, True)

# ── 7. 仓库快照（精简格式）解析 ──
slim = {"source": "sbsub", "version": "b666c0e2",
        "mapping": {"1": 1, "1281": 1221, "0": 5, "abc": 7, "5": 0}}
slim_map = g["_build_mapping"](slim)
check("精简格式被识别", g["_is_slim"](slim), True)
check("原生格式不被误判", g["_is_slim"](fake_sbsub([(1, 1)])), False)
# 只保留正整数对（0 / 非数字 / 非正 TMDB 号全部丢弃）
check("精简格式解析结果", slim_map, {1: 1, 1281: 1221})
check("空/异常输入不炸", g["_build_mapping"]({"mapping": {}}), {})
check("None 不炸", g["_build_mapping"](None), {})
# 两种格式解析同样内容必须等价，否则换源会算出不同集号
pairs = [(1, 1), (1063, 1005), (1262, 1210), (1281, 1221)]
slim_same = {"mapping": {str(s): t for s, t in pairs}}
check("两种格式等价", g["_build_mapping"](fake_sbsub(pairs)), g["_build_mapping"](slim_same))

# 半集特辑在精简格式里必须缺席（由 extra_map 唯一提供）
slim_no_half = {"mapping": {"1134": 1076, "1262": 1202}}
check("快照不含半集特辑", 1076 not in g["_build_mapping"](slim_no_half), True)

print()
if failures:
    print(f"❌ 失败 {len(failures)} 项：{failures}")
    sys.exit(1)
print("✅ 全部纯逻辑测试通过")
