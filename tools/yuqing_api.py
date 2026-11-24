"""舆情平台接口。

调用顺序：登录 → 专题列表 → 文章列表 → 文章详情 → 热点数据 → 专题预警 → 融媒体数据

公共参数在 Config 里改，用户名密码和每次的查询条件在 main() 里改。
运行：python tools/yuqing_api.py
"""

import hashlib
import json
import time
from datetime import datetime, timedelta

import requests
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, modes

try:
    from cryptography.hazmat.decrepit.ciphers.algorithms import TripleDES
except ImportError:
    from cryptography.hazmat.primitives.ciphers.algorithms import TripleDES


class Config:
    base_url = ""          # 域名，如 "https://yq.example.com"
    imei = ""              # 接口方提供的 imei
    sign_key = ""          # 签名密钥
    interval = 6           # 两次请求间隔（秒）：文档限制 1 分钟不超过 10 次
    timeout = 30           # 请求超时（秒）


class YuqingApi:
    def __init__(self, config):
        self.cfg = config
        self.token = ""
        self.user_id = None

    # ---------------------------------------------------------------- 签名
    def sign(self, data, timestamp):
        """data 按字段顺序拼「字段名+值」，再拼 #时间戳，3DES 加密后转十六进制。"""
        text = "".join(f"{k}{'null' if v is None else v}" for k, v in data.items())
        text += f"#{timestamp}"
        padder = padding.PKCS7(64).padder()
        raw = padder.update(text.encode()) + padder.finalize()
        key = self.cfg.sign_key.encode()[:24]
        enc = Cipher(TripleDES(key), modes.ECB()).encryptor()
        return (enc.update(raw) + enc.finalize()).hex()

    # ---------------------------------------------------------------- 请求
    def post(self, path, body):
        headers = {"imei": self.cfg.imei}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        resp = requests.post(self.cfg.base_url + path, json=body, headers=headers,
                             timeout=self.cfg.timeout)
        time.sleep(self.cfg.interval)
        result = resp.json()
        if str(result.get("code")) != "200":
            raise RuntimeError(f"{path} 调用失败：{result.get('code')} {result.get('msg')}")
        return result.get("data")

    # ---------------------------------------------------------------- 1. 登录
    def login(self, login_name, password):
        data = {"loginName": login_name,
                "passWord": hashlib.md5(password.encode()).hexdigest()}
        ts = int(time.time() * 1000)
        body = {"sign": self.sign(data, ts), "timestamp": ts, "data": data}
        result = self.post("/api/third/login", body)
        self.token = result["token"]
        self.user_id = result["userId"]
        print(f"登录成功，userId={self.user_id}")

    # ---------------------------------------------------------------- 2. 专题列表
    def special_list(self):
        return self.post("/api/third/special/list", {"userId": self.user_id})

    # ---------------------------------------------------------------- 3. 文章列表
    def doc_list(self, begin, end, page_no=1, page_size=200, **filters):
        body = {"userId": self.user_id, "begin": begin, "end": end,
                "pageNo": page_no, "pageSize": page_size, **filters}
        return self.post("/api/third/special/queryDocList", body)["result"]

    # ---------------------------------------------------------------- 4. 文章详情
    def article_detail(self, ids, actile_ids):
        body = {"userId": self.user_id, "ids": ",".join(ids),
                "actileIds": ",".join(actile_ids)}
        return self.post("/api/third/article/detail", body)

    # ---------------------------------------------------------------- 5. 热点数据
    def hot_top_n(self, begin_time, end_time, top_n=50):
        body = {"userId": self.user_id, "beginTime": begin_time,
                "endTime": end_time, "topN": top_n}
        return self.post("/api/third/special/hotTopN", body)

    # ---------------------------------------------------------------- 6. 专题预警
    def warning(self, page_no=1, page_size=50):
        body = {"userId": self.user_id, "pageNo": page_no, "pageSize": page_size}
        return self.post("/api/third/special/warning", body)

    # ---------------------------------------------------------------- 7. 融媒体数据
    def fusion_media(self, begin, end, page_no=1, page_size=200, **filters):
        body = {"userId": self.user_id, "begin": begin, "end": end,
                "pageNo": page_no, "pageSize": page_size, **filters}
        return self.post("/api/third/fusionMedia/queryList", body)["result"]


def fetch_all(fetch, page_size, max_pages=50):
    """逐页拉取，某页不满 page_size 就说明拉完了。"""
    rows = []
    for page_no in range(1, max_pages + 1):
        page = fetch(page_no, page_size)
        rows += page
        print(f"  第 {page_no} 页 {len(page)} 条")
        if len(page) < page_size:
            break
    return rows


def save(name, rows):
    with open(f"yuqing_{name}.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"{name}：{len(rows)} 条，已保存 yuqing_{name}.json")


def main():
    # ---- 用户名密码 ----
    login_name = ""
    password = ""

    # ---- 查询条件 ----
    end = datetime.now()
    begin = end - timedelta(days=1)                       # 最近 1 天
    begin, end = begin.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")
    doc_filters = {}          # 如 {"groupId": "19", "sen": "-1", "specialId": "4865"}
    fusion_filters = {}       # 如 {"keywordsGroupList": ["起火,消防"], "areaCode": "35"}

    api = YuqingApi(Config)

    # 1. 登录
    api.login(login_name, password)

    # 2. 专题列表
    save("specials", api.special_list())

    # 3. 文章列表
    docs = fetch_all(lambda no, size: api.doc_list(begin, end, no, size, **doc_filters), 200)
    save("docs", docs)

    # 4. 文章详情（每次最多 200 篇）
    details = []
    for i in range(0, len(docs), 200):
        batch = docs[i:i + 200]
        details += api.article_detail([d["id"] for d in batch], [d["actileId"] for d in batch])
    save("details", details)

    # 5. 热点数据
    save("hot", api.hot_top_n(begin, end, top_n=50))

    # 6. 专题预警
    save("warnings", fetch_all(api.warning, 50, max_pages=10))

    # 7. 融媒体数据
    fusion = fetch_all(lambda no, size: api.fusion_media(begin, end, no, size, **fusion_filters), 200)
    save("fusion", fusion)


if __name__ == "__main__":
    main()
