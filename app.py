import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import os

# --- Google API 설정 (동일) ---
SERVICE_ACCOUNT_FILE = 'credentials.json'
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]
SHEET_URL = "https://docs.google.com/spreadsheets/d/1dvx7XQDZCp1f60bdoEi6KcUGpvghO3kxdGZJkj_ZSiE"

# --- 인증 함수 (동일) ---
@st.cache_resource
def authorize_gspread():
    """Google Sheets API 인증 및 클라이언트 객체 반환"""
    creds = None
    if os.path.exists(SERVICE_ACCOUNT_FILE):
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    elif hasattr(st, 'secrets') and "gcp_service_account" in st.secrets:
        creds_dict = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:
        st.error(f"'{SERVICE_ACCOUNT_FILE}' 파일을 찾을 수 없거나 Streamlit Secrets가 설정되지 않았습니다.")
        return None
    try:
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"gspread 인증 중 오류 발생: {e}")
        return None

# --- [수정됨] 간단한 데이터 로드 함수 ---
@st.cache_data(ttl=300) # 5분마다 데이터 새로고침
def load_simple_data(_client, sheet_url, sheet_name="클러스터별"):
    """
    지정된 시트의 데이터를 그대로 DataFrame으로 로드합니다.
    """
    if _client is None: return pd.DataFrame()
    try:
        spreadsheet = _client.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records() # 시트 전체 데이터를 딕셔너리 리스트로 가져옴
        df = pd.DataFrame(data)
        
        # '누적시간' 컬럼을 숫자로 변환 (계산을 위해)
        if '누적시간' in df.columns:
            df['누적시간'] = pd.to_numeric(df['누적시간'], errors='coerce').fillna(0)
            
        return df

    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{sheet_name}' 시트를 찾을 수 없습니다. 시트 이름을 확인하세요.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"시트 로드 중 오류 발생: {e}")
        return pd.DataFrame()

# --- [수정됨] Streamlit 웹 앱 구성 (심플 뷰) ---

# 1. 페이지 기본 설정
st.set_page_config(
    page_title="RAW 데이터 조회",
    page_icon="📄",
    layout="wide"
)

st.title("📄 RAW 데이터 조회 (기본 뷰)")
st.write(f"대상 시트: {SHEET_URL}")

# 2. Google Sheets 클라이언트 인증
client = authorize_gspread()

# 3. 인증 성공 시 앱 실행
if client:
    # 4. 'RAW' 시트 데이터 로드 (시트 이름이 다르면 여기서 수정)
    df_raw = load_simple_data(client, SHEET_URL, sheet_name="클러스터별")

    if not df_raw.empty:
        
        # 5. --- 표시할 컬럼 정의 ---
        # 사용자가 요청한 5개 컬럼
        display_columns = ["클러스터", "사번", "이름", "부서명", "누적시간"]
        
        # 실제 시트에 존재하는 컬럼만 필터링 (오류 방지)
        available_columns = [col for col in display_columns if col in df_raw.columns]
        
        # 만약 요청한 컬럼 중 일부가 시트에 없다면 경고 표시
        missing_columns = [col for col in display_columns if col not in df_raw.columns]
        if missing_columns:
            st.warning(f"시트에서 다음 컬럼을 찾을 수 없습니다: {', '.join(missing_columns)}")

        if available_columns:
            # 6. --- 데이터 테이블 표시 ---
            st.header("RAW 데이터")
            
            # 사이드바에 간단한 필터 추가 (클러스터 선택)
            st.sidebar.header("필터")
            all_clusters = ['전체'] + sorted(df_raw['클러스터'].unique().tolist())
            selected_cluster = st.sidebar.selectbox("클러스터 선택:", all_clusters)
            
            # 필터 적용
            if selected_cluster == '전체':
                df_display = df_raw[available_columns]
            else:
                df_display = df_raw[df_raw['클러스터'] == selected_cluster][available_columns]

            st.dataframe(df_display, use_container_width=True, height=600)
            
            # (선택 사항) '누적시간' 기반 간단한 요약
            st.header("간단 요약")
            avg_hours = df_display['누적시간'].mean()
            st.metric("평균 누적 시간", f"{avg_hours:.1f} 시간")

        else:
            st.error("요청하신 컬럼이 시트에 하나도 존재하지 않습니다. 'RAW' 시트의 헤더를 확인하세요.")

    else:
        st.error("'RAW' 시트에서 데이터를 불러오는 데 실패했거나 데이터가 없습니다.")
else:
    st.warning("Google Sheets 인증에 실패했습니다. 'credentials.json' 파일을 확인하세요.")