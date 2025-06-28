import sys
import pathlib
import streamlit as st

# Add project root to Python path to import config module
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from src.utils import add_custom_css

st.set_page_config(
    page_icon="🌎",
    layout="wide",
    initial_sidebar_state='expanded',
)

css_path = pathlib.Path("app/assets/styles.css")
add_custom_css(css_path)

def main():
    about_page = st.Page("src/pages/page1.py", title="About this project")
    dashboard_page = st.Page("src/pages/dashboard.py", title="Dashboard")

    pg = st.navigation([dashboard_page, about_page])
    pg.run()


if __name__ == "__main__":
    main()
