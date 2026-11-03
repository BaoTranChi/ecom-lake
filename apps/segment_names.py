"""
Đặt tên phân khúc RFM tự động theo luật, để thứ tự cụm ngẫu nhiên của K-Means không làm sai tên.

Luật (dựa trên hồ sơ trung bình của từng cụm):
  1. Điểm giá trị = z(avg_monetary) + z(avg_frequency) - z(avg_recency_days)
  2. Cụm điểm cao nhất                          -> "VIP"
  3. Trong các cụm còn lại, recency nhỏ nhất     -> "Khách mới / vừa mua"
  4. Còn lại: monetary >= trung vị các cụm       -> "Nguy cơ rời bỏ (giá trị cao)"
             monetary <  trung vị               -> "Ngủ đông (giá trị thấp)"
Nếu conf/segment_actions.json đã đặt tên khác "Cần đặt tên" cho một cụm thì tên trong conf được ưu tiên
(người phân tích có quyền điều chỉnh sau khi xem hồ sơ).
"""
import json
import os
import statistics

ACTIONS = {
    "VIP": ("Mời tham gia chương trình thành viên hạng cao, ưu tiên chăm sóc khách hàng", "email"),
    "Khách mới / vừa mua": ("Gợi ý sản phẩm thường mua cùng để tăng giá trị đơn tiếp theo", "push"),
    "Nguy cơ rời bỏ (giá trị cao)": ("Gửi voucher 10% hết hạn sau 7 ngày để kéo khách quay lại", "email+push"),
    "Ngủ đông (giá trị thấp)": ("Đưa vào chiến dịch nhắc nhở chi phí thấp (newsletter hàng tuần)", "email"),
}


def _z(values):
    m, s = statistics.fmean(values), statistics.pstdev(values) or 1.0
    return [(v - m) / s for v in values]


def auto_names(profile_rows: list[dict]) -> dict[str, str]:
    """profile_rows: các dòng của segment_profile.csv (dict). Trả về {segment(str): tên}."""
    if not profile_rows:
        return {}
    segs = [str(int(float(r["segment"]))) for r in profile_rows]
    mon = [float(r["avg_monetary"]) for r in profile_rows]
    fre = [float(r["avg_frequency"]) for r in profile_rows]
    rec = [float(r["avg_recency_days"]) for r in profile_rows]
    score = [a + b - c for a, b, c in zip(_z(mon), _z(fre), _z(rec))]
    names, remaining = {}, list(range(len(segs)))
    vip = max(remaining, key=lambda i: score[i])
    names[segs[vip]] = "VIP"
    remaining.remove(vip)
    if remaining:
        new = min(remaining, key=lambda i: rec[i])
        names[segs[new]] = "Khách mới / vừa mua"
        remaining.remove(new)
    med = statistics.median(mon)
    for i in remaining:
        base = "Nguy cơ rời bỏ (giá trị cao)" if mon[i] >= med else "Ngủ đông (giá trị thấp)"
        name, k = base, 2
        while name in names.values():          # k > 4: tránh trùng tên
            name, k = f"{base} {k}", k + 1
        names[segs[i]] = name
    return names


def resolve(profile_rows: list[dict], conf_path: str) -> dict[str, dict]:
    """Kết hợp tên tự động + tên người dùng đặt trong conf. Trả về {segment: {name, action, channel, source}}."""
    conf = {}
    if os.path.exists(conf_path):
        with open(conf_path) as f:
            conf = json.load(f).get("segments", {})
    auto = auto_names(profile_rows)
    out = {}
    for seg, name in auto.items():
        c = conf.get(seg, {})
        if c.get("name") and not c["name"].startswith("Cần đặt tên"):
            out[seg] = {"name": c["name"], "action": c.get("action", ""), "channel": c.get("channel", ""),
                        "source": "conf"}
        else:
            base = name.rstrip(" 0123456789")
            act, ch = ACTIONS.get(base, ("", ""))
            out[seg] = {"name": name, "action": act, "channel": ch, "source": "auto"}
    return out
