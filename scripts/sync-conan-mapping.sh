#!/usr/bin/env bash
# 把 sbsub 柯南映射同步成本仓库的精简快照（conan-mapping/mapping.json）。
#
# 用途：ConanRename 插件在主源（cloud.sbsub.com）不可用时的后备映射。
# 由本机 cron 定时执行，内容有变化才提交推送；「增量」由 git diff 天然给出。
#
# 用法：
#   ./scripts/sync-conan-mapping.sh            # 同步并推送（有变化时）
#   ./scripts/sync-conan-mapping.sh --dry-run  # 只生成文件、不提交不推送
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_FILE="$REPO_DIR/conan-mapping/mapping.json"
SRC_URL="${CONAN_MAPPING_SRC:-https://cloud.sbsub.com/data/data.json}"
API_URL="https://api.github.com/repos/libremk66/MoviePilot-Plugins/contents/conan-mapping/mapping.json?ref=main"
DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# 1. 拉主源。失败即退出（不能让空数据覆盖快照）
code=$(curl -sS -o "$TMP/sbsub.json" -w '%{http_code}' -m 30 \
  -A 'Mozilla/5.0 MoviePilot-Plugin/ConanRename' "$SRC_URL" || echo 000)
if [[ "$code" != "200" ]]; then
  echo "拉取 sbsub 失败（HTTP $code），本次不更新" >&2
  exit 1
fi

# 2. 转成精简映射（只留插件真正用到的拆分版号；半集特辑交给 extra_map）
python3 - "$TMP/sbsub.json" "$OUT_FILE" "$SRC_URL" <<'PY'
import json, sys
src, dst, src_url = sys.argv[1], sys.argv[2], sys.argv[3]
raw = json.load(open(src, encoding="utf-8"))
tv = (raw.get("res") or [None])[0]
if not tv or len(tv) < 5 or not isinstance(tv[4], dict):
    sys.exit("sbsub 结构不认识：res[0][4] 不是 dict")
eps = tv[4]

def split_nums(value):
    """与插件 _parse_split_nums 保持一致：'115' / '115-116' / '1202.5'"""
    text = str(value or "").strip()
    if not text:
        return []
    if "-" in text:
        left, _, right = text.partition("-")
        try:
            return list(range(int(float(left)), int(float(right)) + 1))
        except (TypeError, ValueError):
            return []
    try:
        return [int(float(text))]
    except (TypeError, ValueError):
        return []

mapping = {}
for key, item in eps.items():
    if not isinstance(item, list) or not item:
        continue
    try:
        seq = float(key)
    except (TypeError, ValueError):
        continue
    if not seq.is_integer():
        continue          # 半集键（1076.5 / 1202.5）不进快照
    for num in split_nums(item[0]):
        mapping.setdefault(num, int(seq))

if len(mapping) < 100:
    sys.exit(f"映射只有 {len(mapping)} 条，明显异常，拒绝写入")

# 防倒退：新映射的覆盖上限不得低于现有快照，否则说明源退化了
try:
    old = json.load(open(dst, encoding="utf-8")).get("mapping") or {}
    old_max = max((int(k) for k in old), default=0)
except Exception:
    old_max = 0
new_max = max(mapping)
if new_max < old_max:
    sys.exit(f"新映射上限 {new_max} 低于现有快照 {old_max}，拒绝回退")

payload = {
    "source": src_url,
    "version": (raw.get("data") or {}).get("version") or "",
    "note": "由 scripts/sync-conan-mapping.sh 定时生成，请勿手改",
    "mapping": {str(k): v for k, v in sorted(mapping.items())},
}
with open(dst, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=0, sort_keys=False)
    f.write("\n")
print(f"写入 {len(mapping)} 条，上限拆分版 {new_max} → TMDB {mapping[new_max]}")
PY

echo "生成完毕：$OUT_FILE（$(wc -c <"$OUT_FILE") 字节）"
[[ "$DRY_RUN" == "1" ]] && { echo "--dry-run，不提交"; exit 0; }

cd "$REPO_DIR"

# 3. 没变化就结束（cron 静默）
if git diff --quiet -- conan-mapping/mapping.json 2>/dev/null && \
   git ls-files --error-unmatch conan-mapping/mapping.json >/dev/null 2>&1; then
  echo "映射无变化，无需提交"
  exit 0
fi

# 4. 提交推送（需要工作区干净，避免把别的改动一起带上去）
if [[ -n "$(git status --porcelain -- ':!conan-mapping/mapping.json')" ]]; then
  echo "工作区有其它未提交改动，为安全起见不自动提交" >&2
  exit 2
fi

git add conan-mapping/mapping.json
git commit -q -m "柯南映射快照：同步至拆分版 $(python3 -c "
import json;d=json.load(open('conan-mapping/mapping.json'));print(max(int(k) for k in d['mapping']))")"
git push -q origin main
echo "已提交并推送"
