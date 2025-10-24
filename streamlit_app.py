import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import os

# 1. 페이지 기본 설정 (가장 먼저 호출되어야 함)
st.set_page_config(
    page_title="RAW 데이터 조회",
    page_icon="📄",
    layout="wide"
)

# --- Google API 설정 (동일) ---
SERVICE_ACCOUNT_FILE = 'credentials.json'
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]
SHEET_URL = "https://docs.google.com/spreadsheets/d/1dvx7XQDZCp1f60bdoEi6KcUGpvghO3kxdGZJkj_ZSiE"

# --- [수정됨] 인증 함수 ---
@st.cache_resource
def authorize_gspread():
    """Google Sheets API 인증 및 클라이언트 객체 반환"""
    creds = None
    
    # 1. 로컬 환경 (credentials.json 파일 사용)
    if os.path.exists(SERVICE_ACCOUNT_FILE):
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    
    # 2. Streamlit Cloud 배포 환경 (st.secrets 사용)
    # "gcp_service_account" 키 대신, JSON 내용물("private_key")이 있는지 확인
    elif hasattr(st, 'secrets') and "private_key" in st.secrets:
        # st.secrets 자체가 인증서 딕셔너리(JSON 내용물)임
        creds = Credentials.from_service_account_info(st.secrets, scopes=SCOPES)
        
    # 3. 인증 실패
    else:
        st.error("인증 정보를 찾을 수 없습니다.")
        st.error("로컬에서는 'credentials.json' 파일이 필요하고, 배포 시에는 Streamlit Cloud의 'Secrets' 설정이 필요합니다.")
        return None
        
    # 클라이언트 반환
    try:
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"gspread 인증 중 오류 발생: {e}")
        return None

# --- 데이터 로드 함수 (수정 없음) ---
@st.cache_data(ttl=300) # 5분마다 데이터 새로고침
def load_simple_data(_client, sheet_url, sheet_name="클러스터별"):
    if _client is None: return pd.DataFrame()
    try:
        spreadsheet = _client.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        
        if '누적시간' in df.columns:
            df['누적시간'] = pd.to_numeric(df['누적시간'], errors='coerce').fillna(0)
            
        return df

    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{sheet_name}' 시트를 찾을 수 없습니다. 시트 이름을 확인하세요.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"시트 로드 중 오류 발생: {e}")
        return pd.DataFrame()

# --- 웹 앱 구성 (수정 없음) ---

st.title("📄 RAW 데이터 조회 (기본 뷰)")
st.write(f"대상 시트: {SHEET_URL}")

client = authorize_gspread()

if client:
    df_raw = load_simple_data(client, SHEET_URL, sheet_name="클러스터별")

    if not df_raw.empty:
        display_columns = ["클러스터", "사번", "이름", "부서명", "누적시간"]
        available_columns = [col for col in display_columns if col in df_raw.columns]
        
        missing_columns = [col for col in display_columns if col not in df_raw.columns]
        if missing_columns:
            st.warning(f"시트에서 다음 컬럼을 찾을 수 없습니다: {', '.join(missing_columns)}")

        if available_columns:
            st.header("RAW 데이터")
            
            st.sidebar.header("필터")
            all_clusters = ['전체'] + sorted(df_raw['클러스터'].unique().tolist())
            selected_cluster = st.sidebar.selectbox("클러스터 선택:", all_clusters)
            
            if selected_cluster == '전체':
                df_display = df_raw[available_columns]
            else:
                df_display = df_raw[df_raw['클러스터'] == selected_cluster][available_columns]

            st.dataframe(df_display, use_container_width=True, height=600)
            
            st.header("간단 요약")
            avg_hours = df_display['누적시간'].mean()
            st.metric("평균 누적 시간", f"{avg_hours:.1f} 시간")

        else:
            st.error("요청하신 컬럼이 시트에 하나도 존재하지 않습니다. '클러스터별' 시트의 헤더를 확인하세요.")

    else:
        st.error("'클러스터별' 시트에서 데이터를 불러오는 데 실패했거나 데이터가 없습니다.")
else:
    st.warning("Google Sheets 인증에 실패했습니다. (Secrets 설정 또는 로컬 파일 확인)")
