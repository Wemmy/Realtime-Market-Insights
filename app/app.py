import streamlit as st
import page1
import page2
import page3

PAGES = {
    "Market Overview": page1,
    "Stock Performance": page2,
    # "Strategy Simulation": page3
}

st.set_page_config(
    page_title="Hello",
    page_icon="👋",
)

st.sidebar.title('Navigation')
selection = st.sidebar.radio("Go to", list(PAGES.keys()))
page = PAGES[selection]
page.app()