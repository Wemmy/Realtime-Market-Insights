import streamlit as st
import openai
import time
from utils.utils import get_alhpa_vantage_news, show_news,read_data,fetch_generated_content,query_data
from datetime import datetime
import pandas as pd
from plotly import graph_objs as go

def app():
    

    st.header("News Sentiment Summary")
    # Create a two-column layout
    col1, col2 = st.columns(2)
    topics = ['Blockchain', 'Earnings', 'IPO', 'Financial Markets', 'Economy - Fiscal', 'Economy - Monetary', 'Economy - Macro', 'Energy & Transportation','Finance', 'Life Sciences', 'Manufacturing', 'Retail & Wholesale', 'Real Estate & Construction','Technology']
    sentiments = ['Somewhat-Bullish', 'Bullish', 'Neutral', 'Somewhat-Bearish', 'Bearish']

    # Dropdown menu for topic selection
    # In the first column, place the first selectbox
    with col1:
        selected_topic = st.selectbox("Choose a News Topic", topics, index=len(topics)-1)
    # In the second column, place the second selectbox
    with col2:
        selected_sentiment = st.selectbox("Choose a Sentiment", sentiments, index=1)

    news = get_alhpa_vantage_news(search_topic=selected_topic)

    # Initialize session state for content and index if not already set
    if 'display_text' not in st.session_state:
        st.session_state.display_text = ""
    if 'generated_content' not in st.session_state:
        st.session_state.generated_content = ""
    if 'index' not in st.session_state:
        st.session_state.index = 0

    # Dropdown to select type of content
    options = [
        "What happened in the past 24 hours?", 
        "Make some prodections based on the data of past 24 hours."
        ]

    selected_option = st.selectbox("Select content type:", options)

    if selected_option == "What happened in the past 24 hours?":
        with open('prompt/prompt1.txt', 'r') as file:
            prompt = file.read()
        filter_dict = {
            'topics': selected_topic,
            'time_published': int(datetime.strptime("20240510", '%Y%m%d').timestamp()),
            'overall_sentiment_label': selected_sentiment
        }
    if selected_option == "Make some prodections based on the data of past 24 hours.":
        with open('prompt/prompt2.txt', 'r') as file:
            prompt = file.read()
        filter_dict = {
            'topics': selected_topic,
            'time_published': int(datetime.strptime("20240510", '%Y%m%d').timestamp()),
            'overall_sentiment_label': selected_sentiment
        }

    # Button to fetch content and initiate display
    if st.button("Generate Content"):
        st.session_state.display_text = fetch_generated_content(collection='alphavantage', filter_dict=filter_dict,  prompt=prompt)

    # Style the container with CSS for fixed size and scrollable content
    st.markdown(
        """
        <style>
        .chat-container {
            height: 200px;  /* Fixed height */
            width: 100%;    /* Full container width */
            overflow-y: auto;  /* Enable vertical scrolling */
            background-color: #f0f2f6;  /* Light gray background */
            border-radius: 10px;  /* Rounded corners */
            padding: 10px;  /* Padding inside the container */
            border: 2px solid #cccccc;  /* Border for better visibility */
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);  /* Subtle shadow for depth */
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Container to display the gradually typed content
    with st.container():
        st.markdown(
            f"<div class='chat-container'>{st.session_state.display_text}</div>",
            unsafe_allow_html=True
        )

    st.header("Economic Indicators")
    indicators = ['TREASURY_YIELD', 'FEDERAL_FUNDS_RATE', 'CPI',  'RETAIL_SALES','DURABLES','UNEMPLOYMENT','NONFARM_PAYROLL']
    indicator_units = {
        'TREASURY_YIELD':'percent', 
        'FEDERAL_FUNDS_RATE':'percent', 
        'CPI':'index', 
        'RETAIL_SALES':'millions of dollars', 
        'DURABLES':'millions of dollars', 
        'UNEMPLOYMENT':'percent',
        'NONFARM-PAYROLL':'thousands of persons'
    }

    # User selection of indicators
    selected_indicators = st.multiselect('Select up to two Economic Indicators', indicators, 'FEDERAL_FUNDS_RATE')

    # Limit the selection to a maximum of 2
    if len(selected_indicators) > 2:
        st.warning('Please select no more than two indicators.')
        selected_indicators = selected_indicators[:2]

    # Fetch and store data for selected indicators
    indicator_data = {}
    for indicator in selected_indicators:
        indicator_data[indicator] = query_data(indicator)

    # Determine the shortest time range
    min_date = pd.Timestamp('1900-01-01')  # Initial high value
    max_date = pd.Timestamp('2099-12-31')  # Initial low value

    for data in indicator_data.values():
        # Ensure data['date'] is in datetime format
        data['date'] = pd.to_datetime(data['date'])
        min_date = max(min_date, data['date'].min())
        max_date = min(max_date, data['date'].max())

    # Create a figure with secondary y-axis
    fig = go.Figure()

    # Add traces for the selected indicators
    for i, indicator in enumerate(selected_indicators):
        unit = indicator_units.get(indicator, '')
        data = indicator_data[indicator]
        axis_title = f"{indicator} ({unit})"
        if i == 0:  # First selected indicator uses the left y-axis
            fig.add_trace(go.Scatter(x=data['date'], y=data['value'], mode='lines', name=indicator, line=dict( shape='spline', smoothing=1.3)))
            fig.update_layout(yaxis=dict(title=axis_title))
        elif i == 1:  # Second selected indicator uses the right y-axis
            fig.add_trace(go.Scatter(x=data['date'], y=data['value'], mode='lines', name=indicator, line=dict(shape='spline', smoothing=1.3), yaxis='y2'))
            fig.update_layout(yaxis2=dict(title=axis_title, overlaying='y', side='right'))

    # Update layout for secondary y-axis
    fig.update_layout(
        xaxis=dict(
            range=[min_date, max_date]  # Set the range for x-axis
        )
    )
    # Show the plot in Streamlit
    st.plotly_chart(fig, use_container_width=True)

    st.header("News Details")

    # Slice to get [1, 3, 5] and [2, 4, 6]
    news_odd = [news[i] for i in range(10) if i % 2 == 0]
    news_even = [news[i] for i in range(10) if i % 2 != 0]

    # Create two columns
    col1, col2 = st.columns(2)

    with col1:
        show_news(news_odd)
    with col2:
        show_news(news_even)
