import sys
import pathlib
import streamlit as st

# Add project root to Python path 
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent))

def add_custom_css(file_path):
    with open(file_path) as f:
        st.html(f"<style>{f.read()}</style>")

st.set_page_config(
    page_icon="🌎",
    layout="wide",
    initial_sidebar_state='expanded',
)

css_path = pathlib.Path("src/dashboard/assets/styles.css")
add_custom_css(css_path)

def main():
    about_page = st.Page("pages/page1.py", title="About this project")
    dashboard_page = st.Page("pages/dashboard.py", title="Dashboard")

    pg = st.navigation([dashboard_page, about_page])
    pg.run()


if __name__ == "__main__":
    main()
