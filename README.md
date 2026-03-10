# ETF_PDF
# ============================================================
# ✅ 단일 날짜 / 종가 조회 X / 빠른 버전
# ✅ ETF명: '온라인기업정보' 방지 + 상장명(예: RISE 대형고배당10TR, PLUS 한화그룹주 등)로 추출
# ✅ 여러 ETF 넣어도 각 ETF별 시트 생성 (덮어쓰기 방지)
# 파일명: "{TARGET_DATE}기준 ETF PDF 구성종목.xlsx"
# ============================================================

!pip -q install dart_fss pykrx openpyxl lxml beautifulsoup4

import dart_fss as dart
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import time
from pykrx import stock  # 종목명->코드 보조 매핑 및 ETF명(가능한 경우)에만 사용

# ----------------------------
# INPUT
# ----------------------------
TARGET_DATE = "20260204"  # YYYYMMDD
api_key = '2d2aba2bc9f61e60ec6525eddb81825302e28fea'
dart.set_api_key(api_key=api_key)

# ✅ 여러 ETF 코드 넣어도 됨
etf_code = ['0000J0']  # 예: ['0000J0','315960','069500',...]
HEADERS = {'User-agent': 'Mozilla/5.0'}

# ============================================================
# ✅ [사용자 원코드] 시작 ~ merged 그대로
# ============================================================

corp_list = dart.api.filings.get_corp_code()
corp_df = pd.DataFrame.from_dict(corp_list)
corp_df = corp_df.dropna(subset='stock_code').sort_values('modify_date', ascending=False).reset_index(drop=True)
corp_df['done_YN'] = "N"

corp_names = []
for etf in etf_code:
    url = "https://navercomp.wisereport.co.kr/v2/ETF/index.aspx?cmp_cd=" + str(etf)
    html = requests.get(url, headers={'User-agent':'Mozilla/5.0'})
    soup = BeautifulSoup(html.text, "lxml")
    soup = list(soup)[1]

    start_str = str(soup).index("grid_data")
    end_str = str(soup).index("chartDraw")

    soup_list = str(soup)[start_str+12:end_str]
    soup_list = '[' + soup_list + ']'

    end_str = soup_list.index("chart_data")
    soup_list = soup_list[:end_str-3]

    CU_list = []
    while True:
        idx = soup_list.find('}')
        if idx == -1:
            break
        name = soup_list[1:idx]
        soup_list = soup_list[idx+2:]
        CU_list.append(name)
        if len(soup_list) < 10:
            break

    CU_df = pd.DataFrame(CU_list)
    cu_tmp = CU_df[0].str.split(",")

    CU_main = None
    for i in range(0,4):
        CU_main = pd.concat([CU_main, cu_tmp.str[i].str.split(":").str[1]], axis=1)

    CU_main.columns = ["TRD_DT","AGMT_STK_CNT","STK_NM_KOR","ETF_WEIGHT"]
    CU_main["STK_NM_KOR"] = CU_main["STK_NM_KOR"].str.replace('"','')

    for stock_name in CU_main['STK_NM_KOR']:
        if stock_name != '원화현금':
            if stock_name == '현대차':
                corp_names.append('현대자동차')
            elif stock_name == '삼성화재':
                corp_names.append('삼성화재해상보험')
            else:
                corp_names.append(stock_name)

corp = list(set(corp_names))

listed_corp = pd.DataFrame(list(corp), columns=['corp_name'])
merged = pd.merge(corp_df, listed_corp, on='corp_name', how='inner')

# ============================================================
# ✅ 여기부터: 종가 조회 없이 엑셀 생성 + ETF명 상장명 추출 강화
# ============================================================

def safe_sheet_name(name: str) -> str:
    name = re.sub(r'[\[\]\*\?/\\:]', '_', str(name))
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:31] if len(name) > 31 else name

def normalize_stock_name(nm: str) -> str:
    if nm is None:
        return ""
    nm = str(nm).strip()
    if nm == "현대차":
        return "현대자동차"
    if nm == "삼성화재":
        return "삼성화재해상보험"
    return nm

# DART: corp_name -> 6자리 stock_code (merged에서만)
dart_name_to_code = {}
if not merged.empty:
    tmp = merged.dropna(subset=["stock_code"]).copy()
    tmp["stock_code"] = tmp["stock_code"].astype(str).str.zfill(6)
    for _, r in tmp.iterrows():
        n = str(r["corp_name"]).strip()
        c = str(r["stock_code"]).zfill(6)
        if n and n not in dart_name_to_code:
            dart_name_to_code[n] = c

# KRX: 종목명 -> 6자리 티커 (보조)
def safe_ref_business_day(yyyymmdd: str, max_back_days: int = 60):
    try:
        d = datetime.strptime(yyyymmdd, "%Y%m%d") - timedelta(days=1)
    except Exception:
        return None
    for _ in range(max_back_days):
        day = d.strftime("%Y%m%d")
        try:
            tickers = stock.get_market_ticker_list(day, market="ALL")
            if tickers is not None and len(tickers) > 0:
                return day
        except Exception:
            pass
        d -= timedelta(days=1)
    return None

KRX_REF_DATE = safe_ref_business_day(TARGET_DATE)

def build_krx_name_to_ticker(ref_date):
    if not ref_date:
        return {}
    name_to_ticker = {}
    try:
        tickers = stock.get_market_ticker_list(ref_date, market="ALL")
    except Exception:
        tickers = []
        for mkt in ["KOSPI", "KOSDAQ", "KONEX"]:
            try:
                tickers += stock.get_market_ticker_list(ref_date, market=mkt)
            except Exception:
                pass
    for t in tickers or []:
        try:
            nm = stock.get_market_ticker_name(t)
            if nm and nm not in name_to_ticker:
                name_to_ticker[nm] = str(t).zfill(6)
        except Exception:
            pass
    return name_to_ticker

krx_name_to_code = build_krx_name_to_ticker(KRX_REF_DATE)

# ------------------------------------------------------------
# ✅ ETF명 추출 (상장명 우선)
# ------------------------------------------------------------
ETF_NAME_CACHE = {}

def _clean_etf_name(x: str) -> str:
    x = (x or "").strip()
    x = re.sub(r'\s+', ' ', x).strip()
    # 공통 타이틀/잡문 제거
    bad = ["온라인기업정보", "WiseReport", "와이즈리포트", "NAVER", "Naver", "ETF Finder"]
    if any(b in x for b in bad):
        return ""
    # 법적 명칭 뒤에 붙는 흔한 꼬리 제거 (원하면 더 추가 가능)
    x = re.sub(r'증권상장지수투자신탁\(주식\)\s*$', '', x).strip()
    x = re.sub(r'\s*\(주식\)\s*$', '', x).strip()
    # 괄호 안 종목번호만 남는 경우 방지
    if re.fullmatch(r'[\(\)\d\s\-_/]+', x):
        return ""
    return x

def fetch_etf_name_from_investing(etf: str) -> str:
    """
    kr.investing.com/etfs/{code} 의 H1에서 ETF명 추출
    예: '한화 PLUS 한화그룹주증권상장지수투자신탁(주식) (0000J0)' -> '한화 PLUS 한화그룹주'
    """
    try:
        url = f"https://kr.investing.com/etfs/{str(etf).lower()}"
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "lxml")
        h1 = soup.find("h1")
        if not h1:
            return ""
        txt = h1.get_text(" ", strip=True)
        txt = re.sub(r'\s*\([A-Za-z0-9]+\)\s*$', '', txt).strip()  # (0000J0) 제거
        txt = _clean_etf_name(txt)
        # 법적명 꼬리 제거 2차
        txt = re.sub(r'증권상장지수투자신탁\(주식\)\s*$', '', txt).strip()
        return txt
    except Exception:
        return ""

def fetch_etf_name_cached(etf: str) -> str:
    """
    1) etf가 KRX 6자리 숫자면 pykrx ETF명 우선
    2) WiseReport에서 후보 추출
    3) Investing.com fallback (알파뉴메릭 코드에서 특히 유효)
    """
    etf = str(etf)
    if etf in ETF_NAME_CACHE:
        return ETF_NAME_CACHE[etf]

    name = ""

    # 1) KRX 6자리 숫자면 pykrx에서 ETF 상장명
    if re.fullmatch(r"\d{6}", etf) and KRX_REF_DATE:
        try:
            name = stock.get_etf_ticker_name(etf)
            name = _clean_etf_name(name)
        except Exception:
            name = ""

    # 2) WiseReport 페이지에서 추출 (구조 변화 대비 여러 시도)
    if not name:
        try:
            url = "https://navercomp.wisereport.co.kr/v2/ETF/index.aspx?cmp_cd=" + etf
            html = requests.get(url, headers=HEADERS, timeout=30)
            soup = BeautifulSoup(html.text, "lxml")

            # og:title
            og = soup.find("meta", attrs={"property": "og:title"})
            if og and og.get("content"):
                name = _clean_etf_name(og["content"])

            # 헤더 후보
            if not name:
                candidates = []
                for selector in ["h1", "h2", "strong", "title"]:
                    for tag in soup.select(selector):
                        t = _clean_etf_name(tag.get_text(" ", strip=True))
                        if t:
                            candidates.append(t)

                # candidates 중 '온라인기업정보' 제거 및 가장 길고 의미있는 텍스트 선택
                if candidates:
                    candidates = [c for c in set(candidates) if c and "온라인기업정보" not in c]
                    candidates = sorted(candidates, key=lambda s: (("ETF" in s.upper()) + (len(s)/10.0)), reverse=True)
                    if candidates:
                        name = candidates[0]

            # title fallback은 마지막
            name = _clean_etf_name(name)

        except Exception:
            name = ""

    # 3) Investing.com fallback (0000J0 같은 코드 해결용)
    if not name:
        name = fetch_etf_name_from_investing(etf)
        name = _clean_etf_name(name)

    if not name:
        name = f"ETF_{etf}"

    ETF_NAME_CACHE[etf] = name
    return name

# WiseReport grid_data 파싱(원코드 방식) + target_date 없으면 직전 TRD_DT 자동 선택
def fetch_pdf_by_etf_and_date_safe(etf: str, target_date: str) -> pd.DataFrame:
    try:
        url = "https://navercomp.wisereport.co.kr/v2/ETF/index.aspx?cmp_cd=" + str(etf)
        html = requests.get(url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(html.text, "lxml")
        soup = list(soup)[1]

        s = str(soup)
        start_str = s.index("grid_data")
        end_str = s.index("chartDraw")

        soup_list = s[start_str+12:end_str]
        soup_list = '[' + soup_list + ']'

        end_str2 = soup_list.index("chart_data")
        soup_list = soup_list[:end_str2-3]

        CU_list = []
        while True:
            idx = soup_list.find('}')
            if idx == -1:
                break
            name = soup_list[1:idx]
            soup_list = soup_list[idx+2:]
            CU_list.append(name)
            if len(soup_list) < 10:
                break

        if len(CU_list) == 0:
            return pd.DataFrame(columns=["TRD_DT","AGMT_STK_CNT","STK_NM_KOR","ETF_WEIGHT"])

        CU_df = pd.DataFrame(CU_list)
        cu_tmp = CU_df[0].str.split(",")

        CU_main = None
        for i in range(0,4):
            CU_main = pd.concat([CU_main, cu_tmp.str[i].str.split(":").str[1]], axis=1)
        CU_main.columns = ["TRD_DT","AGMT_STK_CNT","STK_NM_KOR","ETF_WEIGHT"]

        CU_main["STK_NM_KOR"] = CU_main["STK_NM_KOR"].astype(str).str.replace('"','').str.strip()
        CU_main["TRD_DT"] = CU_main["TRD_DT"].astype(str).str.replace('"','').str.strip()

        CU_main = CU_main[~CU_main["STK_NM_KOR"].isin(["원화현금", "현금", "원화 현금"])].copy()
        CU_main["AGMT_STK_CNT"] = pd.to_numeric(CU_main["AGMT_STK_CNT"], errors="coerce")
        CU_main["ETF_WEIGHT"] = pd.to_numeric(CU_main["ETF_WEIGHT"], errors="coerce")

        # target_date 이하 중 가장 최근 TRD_DT 선택
        CU_le = CU_main[CU_main["TRD_DT"] <= target_date].copy()
        if CU_le.empty:
            return pd.DataFrame(columns=["TRD_DT","AGMT_STK_CNT","STK_NM_KOR","ETF_WEIGHT"])
        picked = CU_le["TRD_DT"].max()
        return CU_le[CU_le["TRD_DT"] == picked].reset_index(drop=True)

    except Exception:
        return pd.DataFrame(columns=["TRD_DT","AGMT_STK_CNT","STK_NM_KOR","ETF_WEIGHT"])

# 종목코드+종목명 결정 (DART 성공=이름 유지 / KRX만 성공=이름 공백)
def resolve_code_and_name(original_name: str):
    nm = normalize_stock_name(original_name)
    if nm == "" or nm in ["원화현금", "현금", "원화 현금"]:
        return "", ""
    code1 = dart_name_to_code.get(nm, "")
    if code1:
        return str(code1).zfill(6), str(original_name).strip()
    code2 = krx_name_to_code.get(nm, "")
    if code2:
        return str(code2).zfill(6), ""  # ✅ 코드만, 종목명 공백
    return "", ""

# =========================
# 메인 실행 (단일 날짜)
# =========================
per_etf_frames = {}
all_frames = []

for etf in etf_code:
    etf = str(etf)
    etf_name = fetch_etf_name_cached(etf)

    # ✅ 여러 ETF 시트가 덮어쓰지 않게 "ETF명_코드"로 유니크 키 생성
    sheet_key = f"{etf_name}_{etf}"
    sheet_name = safe_sheet_name(sheet_key)

    df_pdf = fetch_pdf_by_etf_and_date_safe(etf, TARGET_DATE)
    if df_pdf.empty:
        out = pd.DataFrame(columns=["ETF코드","ETF명","종목명","주식종목코드(6)","주식수","PDF내 주식 비중(%)"])
        per_etf_frames[sheet_name] = out
        continue

    mapped = df_pdf["STK_NM_KOR"].apply(resolve_code_and_name)
    df_pdf["주식종목코드(6)"] = mapped.apply(lambda x: x[0])
    df_pdf["종목명"] = mapped.apply(lambda x: x[1])

    out = pd.DataFrame({
        "ETF코드": etf,
        "ETF명": etf_name,
        "종목명": df_pdf["종목명"],
        "주식종목코드(6)": df_pdf["주식종목코드(6)"],
        "주식수": df_pdf["AGMT_STK_CNT"],
        "PDF내 주식 비중(%)": df_pdf["ETF_WEIGHT"],
    }).sort_values("PDF내 주식 비중(%)", ascending=False).reset_index(drop=True)

    per_etf_frames[sheet_name] = out
    all_frames.append(out)

    time.sleep(0.05)

summary_df = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()

file_name = f"{TARGET_DATE}기준 ETF PDF 구성종목.xlsx"

with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
    (summary_df if not summary_df.empty else pd.DataFrame({"msg": ["No data"]})).to_excel(
        writer, index=False, sheet_name="Summary"
    )
    for sname, df in per_etf_frames.items():
        df.to_excel(writer, index=False, sheet_name=sname)

print(f"✅ Saved: {file_name}")
print(f"✅ KRX_REF_DATE used for name->ticker mapping: {KRX_REF_DATE}")
print("✅ Sheets:", ["Summary"] + list(per_etf_frames.keys()))
