# -*- coding: utf-8 -*-
"""Stock universe: auto-fetch a broad A-share list with sector metadata."""

import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    QUOTE_CACHE_MAX_AGE_MINUTES,
    REQUEST_TIMEOUT,
    USER_AGENT,
    UNIVERSE_CACHE_PATH,
    UNIVERSE_CACHE_MAX_AGE_HOURS,
)


UNIVERSE_CACHE_VERSION = 3
AUTO_UNIVERSE_PAGE_SIZE = 800
AUTO_UNIVERSE_MARKET_FILTER = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
SINA_UNIVERSE_NODE = "hs_a"
SINA_UNIVERSE_PAGE_SIZE = 1000
SINA_ALLOWED_SYMBOL_PREFIXES = ("sh", "sz")


SECTOR_POOLS: Dict[str, List[Tuple[str, str]]] = {
    "银行": [
        ("600000", "浦发银行"),
        ("600015", "华夏银行"),
        ("600016", "民生银行"),
        ("600036", "招商银行"),
        ("600919", "江苏银行"),
        ("600926", "杭州银行"),
        ("600928", "西安银行"),
        ("601009", "南京银行"),
        ("601166", "兴业银行"),
        ("601169", "北京银行"),
        ("601229", "上海银行"),
        ("601288", "农业银行"),
        ("601328", "交通银行"),
        ("601398", "工商银行"),
        ("601658", "邮储银行"),
        ("601818", "光大银行"),
        ("601838", "成都银行"),
        ("601939", "建设银行"),
        ("601988", "中国银行"),
        ("002142", "宁波银行"),
    ],
    "非银金融": [
        ("300059", "东方财富"),
        ("600030", "中信证券"),
        ("600061", "国投资本"),
        ("600109", "国金证券"),
        ("600837", "海通证券"),
        ("600958", "东方证券"),
        ("600999", "招商证券"),
        ("601066", "中信建投"),
        ("601211", "国泰君安"),
        ("601236", "红塔证券"),
        ("601318", "中国平安"),
        ("601319", "中国人保"),
        ("601336", "新华保险"),
        ("601377", "兴业证券"),
        ("601555", "东吴证券"),
        ("601601", "中国太保"),
        ("601628", "中国人寿"),
        ("601688", "华泰证券"),
        ("601788", "光大证券"),
        ("601878", "浙商证券"),
        ("601881", "中国银河"),
        ("601901", "方正证券"),
        ("002736", "国信证券"),
    ],
    "白酒食品": [
        ("000568", "泸州老窖"),
        ("000596", "古井贡酒"),
        ("000729", "燕京啤酒"),
        ("000858", "五粮液"),
        ("002304", "洋河股份"),
        ("002311", "海大集团"),
        ("002507", "涪陵榨菜"),
        ("002557", "洽洽食品"),
        ("002714", "牧原股份"),
        ("300498", "温氏股份"),
        ("600132", "重庆啤酒"),
        ("600298", "安琪酵母"),
        ("600519", "贵州茅台"),
        ("600600", "青岛啤酒"),
        ("600702", "舍得酒业"),
        ("600779", "水井坊"),
        ("600809", "山西汾酒"),
        ("600872", "中炬高新"),
        ("600887", "伊利股份"),
        ("603027", "千禾味业"),
        ("603288", "海天味业"),
        ("603317", "天味食品"),
        ("603345", "安井食品"),
        ("603369", "今世缘"),
    ],
    "家电家居": [
        ("000333", "美的集团"),
        ("000651", "格力电器"),
        ("002032", "苏泊尔"),
        ("002050", "三花智控"),
        ("002242", "九阳股份"),
        ("002444", "巨星科技"),
        ("002508", "老板电器"),
        ("600690", "海尔智家"),
        ("603195", "公牛集团"),
        ("603486", "科沃斯"),
        ("603801", "志邦家居"),
        ("603816", "顾家家居"),
        ("603833", "欧派家居"),
        ("603899", "晨光股份"),
    ],
    "医药医疗": [
        ("000538", "云南白药"),
        ("000661", "长春高新"),
        ("000963", "华东医药"),
        ("002007", "华兰生物"),
        ("002252", "上海莱士"),
        ("300003", "乐普医疗"),
        ("300015", "爱尔眼科"),
        ("300122", "智飞生物"),
        ("300142", "沃森生物"),
        ("300347", "泰格医药"),
        ("300529", "健帆生物"),
        ("300759", "康龙化成"),
        ("300760", "迈瑞医疗"),
        ("300957", "贝泰妮"),
        ("600085", "同仁堂"),
        ("600161", "天坛生物"),
        ("600196", "复星医药"),
        ("600276", "恒瑞医药"),
        ("600436", "片仔癀"),
        ("603259", "药明康德"),
        ("688271", "联影医疗"),
        ("688363", "华熙生物"),
    ],
    "新能源车": [
        ("000625", "长安汽车"),
        ("002050", "三花智控"),
        ("002074", "国轩高科"),
        ("002129", "TCL中环"),
        ("002240", "盛新锂能"),
        ("002460", "赣锋锂业"),
        ("002466", "天齐锂业"),
        ("002594", "比亚迪"),
        ("002709", "天赐材料"),
        ("002812", "恩捷股份"),
        ("300014", "亿纬锂能"),
        ("300073", "当升科技"),
        ("300124", "汇川技术"),
        ("300274", "阳光电源"),
        ("300316", "晶盛机电"),
        ("300568", "星源材质"),
        ("300750", "宁德时代"),
        ("300763", "锦浪科技"),
        ("300827", "上能电气"),
        ("600733", "北汽蓝谷"),
        ("601633", "长城汽车"),
        ("603659", "璞泰来"),
        ("603799", "华友钴业"),
        ("605117", "德业股份"),
    ],
    "光伏风电": [
        ("000591", "太阳能"),
        ("000883", "湖北能源"),
        ("002056", "横店东磁"),
        ("300274", "阳光电源"),
        ("300316", "晶盛机电"),
        ("600438", "通威股份"),
        ("600905", "三峡能源"),
        ("601012", "隆基绿能"),
        ("601615", "明阳智能"),
        ("601865", "福莱特"),
        ("603185", "弘元绿能"),
        ("603806", "福斯特"),
        ("605117", "德业股份"),
        ("688223", "晶科能源"),
        ("688303", "大全能源"),
        ("688390", "固德威"),
        ("688472", "阿特斯"),
        ("688599", "天合光能"),
    ],
    "半导体电子": [
        ("000100", "TCL科技"),
        ("002049", "紫光国微"),
        ("002179", "中航光电"),
        ("002241", "歌尔股份"),
        ("002371", "北方华创"),
        ("002384", "东山精密"),
        ("002415", "海康威视"),
        ("002475", "立讯精密"),
        ("002916", "深南电路"),
        ("300223", "北京君正"),
        ("300308", "中际旭创"),
        ("300373", "扬杰科技"),
        ("300408", "三环集团"),
        ("300433", "蓝思科技"),
        ("300458", "全志科技"),
        ("300496", "中科创达"),
        ("300502", "新易盛"),
        ("300628", "亿联网络"),
        ("300661", "圣邦股份"),
        ("300782", "卓胜微"),
        ("300857", "协创数据"),
        ("600460", "士兰微"),
        ("600584", "长电科技"),
        ("600703", "三安光电"),
        ("600745", "闻泰科技"),
        ("601138", "工业富联"),
        ("603005", "晶方科技"),
        ("603160", "汇顶科技"),
        ("603290", "斯达半导"),
        ("603501", "韦尔股份"),
        ("603893", "瑞芯微"),
        ("603986", "兆易创新"),
        ("688008", "澜起科技"),
        ("688012", "中微公司"),
        ("688036", "传音控股"),
        ("688041", "海光信息"),
        ("688256", "寒武纪"),
        ("688396", "华润微"),
        ("688981", "中芯国际"),
    ],
    "软件通信": [
        ("000977", "浪潮信息"),
        ("002027", "分众传媒"),
        ("002230", "科大讯飞"),
        ("300033", "同花顺"),
        ("300339", "润和软件"),
        ("300413", "芒果超媒"),
        ("300454", "深信服"),
        ("600050", "中国联通"),
        ("600536", "中国软件"),
        ("600570", "恒生电子"),
        ("600588", "用友网络"),
        ("600845", "宝信软件"),
        ("600941", "中国移动"),
        ("601360", "三六零"),
        ("601698", "中国卫通"),
        ("601728", "中国电信"),
        ("688111", "金山办公"),
    ],
    "高端制造": [
        ("000157", "中联重科"),
        ("000338", "潍柴动力"),
        ("000425", "徐工机械"),
        ("000768", "中航西飞"),
        ("002025", "航天电器"),
        ("002179", "中航光电"),
        ("600031", "三一重工"),
        ("600038", "中直股份"),
        ("600089", "特变电工"),
        ("600150", "中国船舶"),
        ("600312", "平高电气"),
        ("600406", "国电南瑞"),
        ("600482", "中国动力"),
        ("600660", "福耀玻璃"),
        ("600760", "中航沈飞"),
        ("600862", "中航高科"),
        ("600893", "航发动力"),
        ("601100", "恒立液压"),
        ("601117", "中国化学"),
        ("601669", "中国电建"),
        ("601766", "中国中车"),
        ("601868", "中国能建"),
        ("601877", "正泰电器"),
        ("601989", "中国重工"),
    ],
    "周期资源": [
        ("000301", "东方盛虹"),
        ("000630", "铜陵有色"),
        ("000708", "中信特钢"),
        ("000807", "云铝股份"),
        ("000898", "鞍钢股份"),
        ("000933", "神火股份"),
        ("002064", "华峰化学"),
        ("002493", "荣盛石化"),
        ("002601", "龙佰集团"),
        ("002648", "卫星化学"),
        ("600010", "包钢股份"),
        ("600019", "宝钢股份"),
        ("600111", "北方稀土"),
        ("600141", "兴发集团"),
        ("600176", "中国巨石"),
        ("600188", "兖矿能源"),
        ("600309", "万华化学"),
        ("600346", "恒力石化"),
        ("600426", "华鲁恒升"),
        ("600516", "方大炭素"),
        ("600547", "山东黄金"),
        ("600989", "宝丰能源"),
        ("601088", "中国神华"),
        ("601216", "君正集团"),
        ("601225", "陕西煤业"),
        ("601233", "桐昆股份"),
        ("601600", "中国铝业"),
        ("601899", "紫金矿业"),
        ("603225", "新凤鸣"),
        ("603260", "合盛硅业"),
        ("603993", "洛阳钼业"),
    ],
    "能源公用": [
        ("000027", "深圳能源"),
        ("000543", "皖能电力"),
        ("000723", "美锦能源"),
        ("000883", "湖北能源"),
        ("003816", "中国广核"),
        ("600011", "华能国际"),
        ("600023", "浙能电力"),
        ("600025", "华能水电"),
        ("600027", "华电国际"),
        ("600674", "川投能源"),
        ("600795", "国电电力"),
        ("600863", "内蒙华电"),
        ("600875", "东方电气"),
        ("600886", "国投电力"),
        ("600900", "长江电力"),
        ("600938", "中国海油"),
        ("601857", "中国石油"),
        ("601985", "中国核电"),
        ("601991", "大唐发电"),
        ("600028", "中国石化"),
    ],
    "基建交运": [
        ("000002", "万科A"),
        ("000089", "深圳机场"),
        ("001979", "招商蛇口"),
        ("002120", "韵达股份"),
        ("002352", "顺丰控股"),
        ("600004", "白云机场"),
        ("600009", "上海机场"),
        ("600018", "上港集团"),
        ("600026", "中远海能"),
        ("600029", "南方航空"),
        ("600048", "保利发展"),
        ("600115", "中国东航"),
        ("600170", "上海建工"),
        ("600606", "绿地控股"),
        ("600919", "江苏银行"),
        ("601006", "大秦铁路"),
        ("601111", "中国国航"),
        ("601186", "中国铁建"),
        ("601390", "中国中铁"),
        ("601618", "中国中冶"),
        ("601668", "中国建筑"),
        ("601800", "中国交建"),
        ("601866", "中远海发"),
        ("601872", "招商轮船"),
        ("601919", "中远海控"),
        ("603885", "吉祥航空"),
    ],
    "商贸服务": [
        ("000858", "五粮液"),
        ("002558", "巨人网络"),
        ("300144", "宋城演艺"),
        ("300251", "光线传媒"),
        ("300413", "芒果超媒"),
        ("600258", "首旅酒店"),
        ("600637", "东方明珠"),
        ("600827", "百联股份"),
        ("601098", "中南传媒"),
        ("601888", "中国中免"),
        ("601933", "永辉超市"),
        ("603605", "珀莱雅"),
    ],
}


def _build_retry_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(
        max_retries=Retry(
            total=1,
            connect=1,
            read=1,
            status=1,
            backoff_factor=0.2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value in ("", None, "--"):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        if value in ("", None, "--"):
            return default
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return default


def _build_sina_quote_snapshot(item: Dict[str, str], snapshot_saved_at: str) -> Dict[str, object]:
    ticktime = str(item.get("ticktime", "")).strip()
    snapshot_date = snapshot_saved_at[:10]
    updated_at = snapshot_saved_at
    if ticktime and len(snapshot_date) == 10:
        updated_at = f"{snapshot_date}T{ticktime}"

    return {
        "code": str(item.get("code", "")).strip(),
        "name": str(item.get("name", "")).strip(),
        "price": _safe_float(item.get("trade")),
        "prev_close": _safe_float(item.get("settlement")),
        "open": _safe_float(item.get("open")),
        "volume": _safe_int(item.get("volume")),
        "volume_input": _safe_int(item.get("volume")),
        "volume_input_unit": "shares",
        "volume_unit": "shares",
        "amount": _safe_float(item.get("amount")),
        "change_amount": _safe_float(item.get("pricechange")),
        "change_pct": _safe_float(item.get("changepercent")),
        "updated_at": updated_at,
        "source": "universe_snapshot",
    }


def _normalize_eastmoney_items(raw_items) -> List[Dict[str, str]]:
    if isinstance(raw_items, dict):
        items = list(raw_items.values())
    elif isinstance(raw_items, list):
        items = raw_items
    else:
        items = []

    stocks: List[Dict[str, str]] = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("f12", "")).strip()
        name = str(item.get("f14", "")).strip()
        sector = str(item.get("f100", "")).strip() or "auto_broad_eastmoney"
        if code and code not in seen and len(code) == 6:
            seen.add(code)
            stocks.append({"code": code, "name": name, "sector": sector})
    stocks.sort(key=lambda entry: entry["code"])
    return stocks


def _get_hardcoded_sector_map() -> Dict[str, str]:
    sector_map: Dict[str, str] = {}
    for sector, stocks in SECTOR_POOLS.items():
        for code, _ in stocks:
            sector_map.setdefault(code, sector)
    return sector_map


def _fetch_eastmoney_universe() -> List[Dict[str, str]]:
    """Fetch a broad A-share universe with Eastmoney industry metadata."""
    headers = {"User-Agent": USER_AGENT}
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": AUTO_UNIVERSE_PAGE_SIZE,
        "fs": AUTO_UNIVERSE_MARKET_FILTER,
        "fields": "f12,f14,f100",
    }
    session = _build_retry_session()

    try:
        resp = session.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            stocks = _normalize_eastmoney_items(data.get("data", {}).get("diff", []))
            if stocks:
                return stocks
    except Exception:
        pass

    return []


def _fetch_sina_universe() -> List[Dict[str, str]]:
    """Fetch a broad A-share universe from Sina market center pages."""
    session = _build_retry_session()
    headers = {"User-Agent": USER_AGENT}
    sector_map = _get_hardcoded_sector_map()
    count_url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeStockCount"
    page_url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeDataSimple"

    try:
        count_resp = session.get(
            count_url,
            params={"node": SINA_UNIVERSE_NODE},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        count_resp.raise_for_status()
        total_count = int(json.loads(count_resp.text))
    except Exception:
        return []

    page_count = max(1, math.ceil(total_count / SINA_UNIVERSE_PAGE_SIZE))
    snapshot_saved_at = datetime.now().isoformat(timespec="seconds")
    stocks: List[Dict[str, str]] = []
    seen = set()

    for page in range(1, page_count + 1):
        try:
            resp = session.get(
                page_url,
                params={
                    "page": page,
                    "num": SINA_UNIVERSE_PAGE_SIZE,
                    "sort": "symbol",
                    "asc": 1,
                    "node": SINA_UNIVERSE_NODE,
                    "symbol": "",
                    "_s_r_a": "page",
                },
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            items = json.loads(resp.text)
        except Exception:
            continue

        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "")).strip().lower()
            code = str(item.get("code", "")).strip()
            name = str(item.get("name", "")).strip()
            if not symbol.startswith(SINA_ALLOWED_SYMBOL_PREFIXES):
                continue
            if code and code not in seen and len(code) == 6:
                seen.add(code)
                stocks.append(
                    {
                        "code": code,
                        "name": name,
                        "sector": sector_map.get(code, "auto_broad_sina"),
                        "quote_snapshot": _build_sina_quote_snapshot(item, snapshot_saved_at),
                        "quote_snapshot_saved_at": snapshot_saved_at,
                    }
                )

    stocks.sort(key=lambda item: item["code"])
    return stocks


def _load_universe_cache_payload() -> Tuple[List[Dict[str, str]], str, str]:
    """Load cached universe plus its recorded source when fresh."""
    if not UNIVERSE_CACHE_PATH.exists():
        return [], "", ""
    try:
        data = json.loads(UNIVERSE_CACHE_PATH.read_text(encoding="utf-8"))
        if int(data.get("version", 0)) != UNIVERSE_CACHE_VERSION:
            return [], "", ""
        saved = datetime.fromisoformat(data.get("saved_at", "2000-01-01"))
        if datetime.now() - saved <= timedelta(hours=UNIVERSE_CACHE_MAX_AGE_HOURS):
            stocks = data.get("stocks", [])
            if isinstance(stocks, list):
                return stocks, str(data.get("source", "")).strip(), str(data.get("saved_at", "")).strip()
    except Exception:
        pass
    return [], "", ""


def _load_universe_cache() -> List[Dict[str, str]]:
    """Load cached universe if fresh."""
    return _load_universe_cache_payload()[0]


def _snapshot_cache_is_fresh(saved_at: str) -> bool:
    try:
        saved = datetime.fromisoformat(saved_at)
    except Exception:
        return False
    return datetime.now() - saved <= timedelta(minutes=QUOTE_CACHE_MAX_AGE_MINUTES)


def _save_universe_cache(stocks: List[Dict[str, str]], source: str) -> None:
    """Save universe to cache."""
    UNIVERSE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    cached_stocks = [item for item in stocks if isinstance(item, dict)]
    payload = {
        "version": UNIVERSE_CACHE_VERSION,
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "count": len(cached_stocks),
        "stocks": cached_stocks,
    }
    UNIVERSE_CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def get_stock_universe_with_source() -> Tuple[List[Dict[str, str]], str]:
    """Return stock universe plus the source label used for this run."""
    cached, cached_source, cached_saved_at = _load_universe_cache_payload()
    if cached:
        if cached_source == "sina_broad_market" and not _snapshot_cache_is_fresh(cached_saved_at):
            refreshed = _fetch_sina_universe()
            if refreshed:
                _save_universe_cache(refreshed, cached_source)
                return refreshed, f"{cached_source}_live"
        return cached, f"{cached_source or 'auto'}_cache"

    fetched = _fetch_eastmoney_universe()
    if fetched:
        source = "eastmoney_broad_market"
        _save_universe_cache(fetched, source)
        return fetched, f"{source}_live"

    fetched = _fetch_sina_universe()
    if fetched:
        source = "sina_broad_market"
        _save_universe_cache(fetched, source)
        return fetched, f"{source}_live"

    return _get_hardcoded_universe(), "hardcoded_fallback"


def get_stock_universe() -> List[Dict[str, str]]:
    """Return stock universe: broad auto-fetch first, hardcoded fallback second."""
    return get_stock_universe_with_source()[0]


def _get_hardcoded_universe() -> List[Dict[str, str]]:
    """Return the hardcoded list of major A-share stocks."""
    seen = set()
    universe: List[Dict[str, str]] = []
    for sector, stocks in SECTOR_POOLS.items():
        for code, name in stocks:
            if code in seen:
                continue
            seen.add(code)
            universe.append({"code": code, "name": name, "sector": sector})
    return universe


# Keep old function name for compatibility
def get_stock_name_map() -> Dict[str, str]:
    """Return a code -> name mapping for the stock universe."""
    return {item["code"]: item["name"] for item in get_stock_universe()}
