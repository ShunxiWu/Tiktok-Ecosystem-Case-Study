import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime
import plotly.graph_objects as go
from dotenv import load_dotenv
import os
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_mongodb():
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise ValueError("MONGO_URI environment variable not set")
    return MongoClient(uri)

def load_data():
    client = connect_mongodb()
    db = client["tiktok"]
    
    # 加载两个主要问题集合
    unhandled = list(db["unhandled_issues"].find())
    mishandled = list(db["mishandled_issues"].find())
    
    # 转换为DataFrame
    df_unhandled = pd.DataFrame(unhandled)
    df_mishandled = pd.DataFrame(mishandled)
    
    # 添加问题类型标记
    df_unhandled['issue_type'] = 'unhandled'
    df_mishandled['issue_type'] = 'mishandled'
    
    # 合并数据
    df = pd.concat([df_unhandled, df_mishandled])
    
    # 转换日期格式
    if 'creation_date' in df.columns:
        df['creation_date'] = pd.to_datetime(df['creation_date'])
        # 只保留五月的数据
        df = df[df['creation_date'].dt.month == 5]
    
    return df

def analyze_issue_distribution(df):
    # 1. 两种问题类型的比例
    issue_type_counts = df['issue_type'].value_counts()
    
    # 2. 各分类的比例
    category_counts = df['category'].value_counts()
    
    return issue_type_counts, category_counts

def create_time_series_plot(df):
    # 按日期和问题类型统计
    daily_counts = df.groupby(['creation_date', 'issue_type']).size().reset_index(name='count')
    
    fig = px.line(daily_counts, 
                  x='creation_date', 
                  y='count', 
                  color='issue_type',
                  title='Daily Issue Counts by Type',
                  labels={'creation_date': 'Date', 
                         'count': 'Number of Issues'})
    
    # 设置y轴从0开始
    fig.update_layout(yaxis_range=[0, daily_counts['count'].max() + 1])
    
    return fig

def create_category_time_series(df):
    # 按日期和分类统计
    daily_category_counts = df.groupby(['creation_date', 'category']).size().reset_index(name='count')
    
    fig = px.line(daily_category_counts, 
                  x='creation_date', 
                  y='count', 
                  color='category',
                  title='Daily Issue Counts by Category',
                  labels={'creation_date': 'Date', 
                         'count': 'Number of Issues'})
    
    # 设置y轴从0开始
    fig.update_layout(yaxis_range=[0, daily_category_counts['count'].max() + 1])
    
    return fig

def create_daily_summary_table(df):
    # 确保日期格式正确
    df['creation_date'] = pd.to_datetime(df['creation_date']).dt.date
    
    # 按日期分组进行统计
    daily_summary = []
    
    for date, group in df.groupby('creation_date'):
        # 计算该日期的统计数据
        unhandled_count = (group['issue_type'] == 'unhandled').sum()
        mishandled_count = (group['issue_type'] == 'mishandled').sum()
        total_count = len(group)
        
        # 统计该日期的分类分布
        category_dist = group['category'].value_counts().to_dict()
        category_str = ", ".join([f"{k}: {v}" for k, v in category_dist.items()])
        
        # 添加到结果列表
        daily_summary.append({
            'Date': date.strftime('%Y-%m-%d'),
            'Total Issues': total_count,
            'Unhandled Issues': unhandled_count,
            'Mishandled Issues': mishandled_count,
            'Category Distribution': category_str
        })
    
    # 转换为DataFrame
    daily_summary_df = pd.DataFrame(daily_summary)
    
    # 按日期排序
    daily_summary_df = daily_summary_df.sort_values('Date')
    
    return daily_summary_df

def create_daily_time_series_plot(df):
    # 按日期和问题类型统计
    daily_counts = df.groupby([df['creation_date'].dt.date, 'issue_type']).size().reset_index(name='count')
    daily_counts.columns = ['date', 'issue_type', 'count']
    
    fig = px.line(daily_counts, 
                  x='date', 
                  y='count', 
                  color='issue_type',
                  title='Daily Issue Distribution',
                  labels={'date': 'Date', 
                         'count': 'Number of Issues'})
    
    fig.update_layout(yaxis_range=[0, daily_counts['count'].max() + 1])
    return fig

def create_category_time_series_plot(df):
    # 按日期和分类统计
    daily_counts = df.groupby([df['creation_date'].dt.date, 'category']).size().reset_index(name='count')
    daily_counts.columns = ['date', 'category', 'count']
    
    fig = px.line(daily_counts, 
                  x='date', 
                  y='count', 
                  color='category',
                  title='Daily Category Distribution',
                  labels={'date': 'Date', 
                         'count': 'Number of Issues'})
    
    fig.update_layout(yaxis_range=[0, daily_counts['count'].max() + 1])
    return fig

def main():
    st.set_page_config(layout="wide")
    st.title("TikTok Governance Issues Analysis (May 2024)")
    
    # 加载数据
    df = load_data()
    
    # 侧边栏筛选器
    st.sidebar.header("Filters")
    
    # 问题类型筛选
    issue_types = st.sidebar.multiselect(
        "Select Issue Types",
        options=['unhandled', 'mishandled'],
        default=['unhandled', 'mishandled']
    )
    
    # 分类筛选
    categories = st.sidebar.multiselect(
        "Select Categories",
        options=df['category'].unique(),
        default=df['category'].unique()
    )
    
    # 日期范围筛选（默认五月）
    if 'creation_date' in df.columns:
        min_date = df['creation_date'].min()
        max_date = df['creation_date'].max()
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
    
    # 应用筛选
    filtered_df = df[
        (df['issue_type'].isin(issue_types)) &
        (df['category'].isin(categories))
    ]
    
    if 'creation_date' in df.columns:
        filtered_df = filtered_df[
            (filtered_df['creation_date'].dt.date >= date_range[0]) &
            (filtered_df['creation_date'].dt.date <= date_range[1])
        ]
    
    # 显示统计数据
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Issue Type Distribution")
        issue_type_counts = filtered_df['issue_type'].value_counts()
        fig1 = px.pie(values=issue_type_counts.values, 
                     names=issue_type_counts.index,
                     title="Unhandled vs Mishandled Issues")
        st.plotly_chart(fig1)
    
    with col2:
        st.subheader("Category Distribution")
        category_counts = filtered_df['category'].value_counts()
        fig2 = px.pie(values=category_counts.values, 
                     names=category_counts.index,
                     title="Issue Categories")
        st.plotly_chart(fig2)
    
    # 时间序列图表
    st.subheader("Issue Trends Analysis")
    col3, col4 = st.columns(2)
    
    with col3:
        st.markdown("**Daily Issue Type Distribution**")
        fig3 = create_daily_time_series_plot(filtered_df)
        st.plotly_chart(fig3, use_container_width=True)
    
    with col4:
        st.markdown("**Daily Category Distribution**")
        fig4 = create_category_time_series_plot(filtered_df)
        st.plotly_chart(fig4, use_container_width=True)
    
    # 显示每日汇总表格
    st.subheader("Daily Issue Summary")
    daily_summary = create_daily_summary_table(filtered_df)

    # 添加总计行
    total_row = pd.DataFrame({
        'Date': ['Total'],
        'Total Issues': [daily_summary['Total Issues'].sum()],
        'Unhandled Issues': [daily_summary['Unhandled Issues'].sum()],
        'Mishandled Issues': [daily_summary['Mishandled Issues'].sum()],
        'Category Distribution': ['Overall Summary']
    })
    daily_summary = pd.concat([daily_summary, total_row], ignore_index=True)

    # 显示表格
    st.dataframe(daily_summary, use_container_width=True)

    # 显示详细数据
    st.subheader("Detailed Issue Data")
    st.dataframe(filtered_df[['tweet_id', 'creation_date', 'text', 'issue_type', 
                             'category', 'retweet_count', 'favorite_count']])

if __name__ == "__main__":
    main()