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

from html import escape

import unicodedata
import re

def clean_text(text):
    """清除控制字符并转义 HTML，避免非法字符导致渲染崩溃。"""
    if pd.isna(text):
        return ""
    if not isinstance(text, str):
        text = str(text)
    text = unicodedata.normalize("NFKD", text)                  # 标准化为兼容形式
    text = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", text)            # 去除控制字符
    return escape(text)                                         # HTML 转义

def render_custom_table(df):
    styles = """
    <style>
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 14px;
    }
    th, td {
        border: 1px solid #ddd;
        padding: 8px;
        vertical-align: top;
        text-align: left;
        word-break: break-word;
    }
    th {
        background-color: #f2f2f2;
    }
    .narrow {
        width: 60px;
        text-align: right;
    }
    .wide-text {
        max-width: 600px;
    }
    </style>
    """

    columns = ['creation_date', 'text', 'issue_type', 'category', 'keyword', 'retweet_count', 'favorite_count']
    table_html = "<table><thead><tr>"
    
    for col in columns:
        table_html += f"<th>{escape(str(col))}</th>"
    table_html += "</tr></thead><tbody>"

    for idx, row in df.iterrows():
        try:
            table_html += "<tr>"
            for col in columns:
                class_attr = ""
                if col == 'creation_date' and isinstance(row[col], pd.Timestamp):
                    val = row[col].strftime('%Y-%m-%d %H:%M')
                else:
                    val = clean_text(row.get(col, ""))

                if col == 'text':
                    class_attr = 'class="wide-text"'
                elif col in ['retweet_count', 'favorite_count']:
                    class_attr = 'class="narrow"'

                # 最后一层防御：避免非法字符干扰 HTML 渲染
                val = escape(str(val)).replace('\x00', '')
                table_html += f"<td {class_attr}>{val}</td>"
            table_html += "</tr>"
        except Exception as e:
            logger.warning(f"Error rendering row {idx}: {e}")
            continue

    table_html += "</tbody></table>"

    # 可选：移除任何乱码字符
    try:
        table_html = table_html.encode('ascii', errors='ignore').decode()
    except:
        pass

    st.markdown(styles + table_html, unsafe_allow_html=True)


def create_category_issue_table(df):
    # 统计每个 category 的 unhandled 和 mishandled 数量
    grouped = df.groupby(['category', 'issue_type']).size().unstack(fill_value=0)
    grouped = grouped.rename(columns={'unhandled': 'Unhandled', 'mishandled': 'Mishandled'})
    
    # 计算总数
    grouped['Total'] = grouped.sum(axis=1)
    
    # 生成百分比文本列
    grouped['Unhandled'] = grouped.apply(lambda row: f"{row['Unhandled']} ({round(row['Unhandled'] / row['Total'] * 100, 1)}%)" if row['Total'] > 0 else "0 (0.0%)", axis=1)
    grouped['Mishandled'] = grouped.apply(lambda row: f"{row['Mishandled']} ({round(row['Mishandled'] / row['Total'] * 100, 1)}%)" if row['Total'] > 0 else "0 (0.0%)", axis=1)
    
    # 重设索引、调整列顺序
    grouped = grouped.reset_index()[['category', 'Unhandled', 'Mishandled', 'Total']]
    
    return grouped


def create_today_hourly_category_plot(df):
    today = datetime.utcnow().date()
    today_str = today.strftime("%B %d, %Y")

    df_today = df[df['creation_date'].dt.date == today].copy()
    if df_today.empty:
        fig = go.Figure()
        fig.update_layout(title=f"No data by category for today ({today_str})",
                          xaxis_title="Hour",
                          yaxis_title="Issue Count")
        return fig

    df_today['hour'] = df_today['creation_date'].dt.hour

    # 分组计数
    hourly_cat = df_today.groupby(['hour', 'category']).size().reset_index(name='count')

    # 补齐小时和 category 组合
    all_hours = pd.DataFrame({'hour': range(24)})
    all_cats = df_today['category'].dropna().unique()
    filled = pd.concat([
        all_hours.assign(category=cat)
        for cat in all_cats
    ])
    hourly_cat = pd.merge(filled, hourly_cat, how='left', on=['hour', 'category']).fillna(0)
    hourly_cat['count'] = hourly_cat['count'].astype(int)

    # 画图
    fig = px.line(hourly_cat,
                  x='hour',
                  y='count',
                  color='category',
                  markers=True,
                  title=f"Hourly Category Flow for Today ({today_str})",
                  labels={'hour': 'Hour of Day', 'count': 'Number of Issues'})

    fig.update_layout(xaxis=dict(tickmode='linear', dtick=1),
                      yaxis_range=[0, hourly_cat['count'].max() + 1])
    return fig


def connect_mongodb():
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise ValueError("MONGO_URI environment variable not set")
    return MongoClient(uri)

def contains_illegal_char(value):
    try:
        if isinstance(value, dict):
            return any(contains_illegal_char(v) for v in value.values())
        elif isinstance(value, list):
            return any(contains_illegal_char(item) for item in value)
        elif isinstance(value, str):
            return '\uFFFD' in value
    except:
        return False
    return False

def clean_illegal_rows(df):
    def is_row_clean(row):
        for col in row.index:
            val = row[col]
            if contains_illegal_char(val):
                return False
        return True
    return df[df.apply(is_row_clean, axis=1)]

def load_data():
    client = connect_mongodb()
    db = client["tiktok"]

    # 设定需要的字段（projection）
    projection = {
        '_id': 0,
        'creation_date': 1,
        'text': 1,
        'category': 1,
        'keyword': 1,
        'retweet_count': 1,
        'favorite_count': 1
    }

    # 设定日期范围（5月1日至5月31日）
    start_date = datetime(2025, 5, 1)
    end_date = datetime(2025, 5, 31, 23, 59, 59)

    # MongoDB查询条件
    query = {"creation_date": {"$gte": start_date, "$lte": end_date}}

    # 分别查询
    unhandled = list(db["unhandled_issues"].find(query, projection))
    mishandled = list(db["mishandled_issues"].find(query, projection))

    # 转换为 DataFrame 并加标签
    df_unhandled = pd.DataFrame(unhandled)
    df_mishandled = pd.DataFrame(mishandled)
    df_unhandled['issue_type'] = 'unhandled'
    df_mishandled['issue_type'] = 'mishandled'

    # 合并
    df = pd.concat([df_unhandled, df_mishandled], ignore_index=True)

    # 转换日期
    if 'creation_date' in df.columns:
        df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')

    # 清理非法字符
    df = clean_illegal_rows(df)

    return df



def create_today_hourly_flow_plot(df):
    # 获取今天的日期（UTC）
    today = datetime.utcnow().date()
    today_str = today.strftime("%B %d, %Y")  # e.g., May 04, 2025

    # 只选取今天的数据
    df_today = df[df['creation_date'].dt.date == today].copy()

    if df_today.empty:
        fig = go.Figure()
        fig.update_layout(title=f"No data for today ({today_str})",
                          xaxis_title="Hour",
                          yaxis_title="Issue Count")
        return fig

    # 提取小时字段
    df_today['hour'] = df_today['creation_date'].dt.hour

    # 按小时和类型分组统计数量
    hourly_counts = df_today.groupby(['hour', 'issue_type']).size().reset_index(name='count')

    # 补齐0-23小时的空值
    all_hours = pd.DataFrame({'hour': range(24)})
    all_types = df_today['issue_type'].unique()
    filled = pd.concat([
        all_hours.assign(issue_type=issue_type)
        for issue_type in all_types
    ])
    hourly_counts = pd.merge(filled, hourly_counts, how='left', on=['hour', 'issue_type']).fillna(0)
    hourly_counts['count'] = hourly_counts['count'].astype(int)

    # 绘图
    fig = px.line(hourly_counts,
                  x='hour',
                  y='count',
                  color='issue_type',
                  markers=True,
                  title=f"Hourly Issue Flow for Today ({today_str})",
                  labels={'hour': 'Hour of Day', 'count': 'Number of Issues'})

    fig.update_layout(xaxis=dict(tickmode='linear', dtick=1),
                      yaxis_range=[0, hourly_counts['count'].max() + 1])
    return fig


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

def create_category_time_series_plot(df):
    df['date'] = df['creation_date'].dt.date
    all_dates = pd.date_range(df['date'].min(), df['date'].max()).date
    all_cats = df['category'].dropna().unique()

    # 构造全量日期 × category 笛卡尔积
    full_grid = pd.MultiIndex.from_product([all_dates, all_cats], names=['date', 'category']).to_frame(index=False)

    # 实际统计
    counts = df.groupby(['date', 'category']).size().reset_index(name='count')
    merged = pd.merge(full_grid, counts, on=['date', 'category'], how='left').fillna(0)
    merged['count'] = merged['count'].astype(int)

    # 加入总数与百分比
    total_per_day = merged.groupby('date')['count'].sum().reset_index(name='total')
    merged = pd.merge(merged, total_per_day, on='date')
    merged['percentage'] = (merged['count'] / merged['total'] * 100).round(2)
    merged['percentage'] = merged['percentage'].fillna(0)

    # 画图
    fig = px.line(
        merged,
        x='date',
        y='percentage',
        color='category',
        title='Daily Issue Category Proportion (%)',
        labels={'date': 'Date', 'percentage': 'Proportion (%)'},
        hover_data={'count': True, 'percentage': True, 'total': True}
    )

    # 添加警戒线
    for y in [80, 20]:
        fig.add_shape(
            type="line",
            x0=merged['date'].min(),
            x1=merged['date'].max(),
            y0=y,
            y1=y,
            line=dict(color="red", width=1, dash="dash"),
            xref="x",
            yref="y"
        )
        fig.add_annotation(
            x=merged['date'].max(),
            y=y,
            text=f"{y}%",
            showarrow=False,
            yanchor="bottom" if y == 80 else "top",
            font=dict(color="red", size=12)
        )

    fig.update_layout(yaxis_range=[0, 100])
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
    df['date'] = df['creation_date'].dt.date
    all_dates = pd.date_range(df['date'].min(), df['date'].max()).date
    all_types = df['issue_type'].dropna().unique()

    # 构造全量时间 × 类型 笛卡尔积
    full_grid = pd.MultiIndex.from_product([all_dates, all_types], names=['date', 'issue_type']).to_frame(index=False)

    # 实际数量统计
    counts = df.groupby(['date', 'issue_type']).size().reset_index(name='count')
    merged = pd.merge(full_grid, counts, on=['date', 'issue_type'], how='left').fillna(0)
    merged['count'] = merged['count'].astype(int)

    # 计算每日总数与百分比
    total_per_day = merged.groupby('date')['count'].sum().reset_index(name='total')
    merged = pd.merge(merged, total_per_day, on='date')
    merged['percentage'] = (merged['count'] / merged['total'] * 100).round(2)
    merged['percentage'] = merged['percentage'].fillna(0)

    # 画图
    fig = px.line(
        merged,
        x='date',
        y='percentage',
        color='issue_type',
        title='Daily Issue Type Proportion (%)',
        labels={'date': 'Date', 'percentage': 'Proportion (%)'},
        hover_data={'count': True, 'percentage': True, 'total': True}
    )

    # 添加 80% 和 20% 的警戒线
    for y in [80, 20]:
        fig.add_shape(
            type="line",
            x0=merged['date'].min(),
            x1=merged['date'].max(),
            y0=y,
            y1=y,
            line=dict(color="red", width=1, dash="dash"),
            xref="x",
            yref="y"
        )
        fig.add_annotation(
            x=merged['date'].max(),
            y=y,
            text=f"{y}%",
            showarrow=False,
            yanchor="bottom" if y == 80 else "top",
            font=dict(color="red", size=12)
        )

    fig.update_layout(yaxis_range=[0, 100])
    return fig




def create_category_raw_count_plot(df):
    df['date'] = df['creation_date'].dt.date
    daily_category_counts = df.groupby(['date', 'category']).size().reset_index(name='count')
    fig = px.line(daily_category_counts, 
                  x='date', 
                  y='count', 
                  color='category',
                  title='Daily Issue Category Raw Count',
                  labels={'date': 'Date', 'count': 'Number of Issues'})
    fig.update_layout(yaxis_range=[0, daily_category_counts['count'].max() + 1])
    return fig



def create_daily_type_count_plot(df):
    df['date'] = df['creation_date'].dt.date

    # 所有日期 + 所有类型的笛卡尔积
    all_dates = pd.date_range(df['date'].min(), df['date'].max()).date
    all_types = df['issue_type'].dropna().unique()
    full_grid = pd.MultiIndex.from_product([all_dates, all_types], names=['date', 'issue_type']).to_frame(index=False)

    # 实际计数
    counts = df.groupby(['date', 'issue_type']).size().reset_index(name='count')

    # 合并并补零
    merged = pd.merge(full_grid, counts, on=['date', 'issue_type'], how='left').fillna(0)
    merged['count'] = merged['count'].astype(int)

    # 画图
    fig = px.line(
        merged,
        x='date',
        y='count',
        color='issue_type',
        title='Daily Issue Type Trend (Raw Count)',
        labels={'date': 'Date', 'count': 'Number of Issues'},
        hover_data={'count': True}
    )
    fig.update_layout(yaxis_range=[0, merged['count'].max() + 1])
    return fig


def create_daily_type_percentage_plot(df):
    daily_counts = df.groupby([df['creation_date'].dt.date, 'issue_type']).size().reset_index(name='count')
    daily_counts.columns = ['date', 'issue_type', 'count']

    total_per_day = daily_counts.groupby('date')['count'].sum().reset_index(name='total')
    merged = pd.merge(daily_counts, total_per_day, on='date')
    merged['percentage'] = (merged['count'] / merged['total'] * 100).round(2)

    fig = px.line(
        merged,
        x='date',
        y='percentage',
        color='issue_type',
        title='Daily Issue Type Trend (Percentage)',
        labels={'date': 'Date', 'percentage': 'Proportion (%)'},
        hover_data={'count': True, 'percentage': True, 'total': True}
    )
    fig.update_layout(yaxis_range=[0, 100])
    return fig


def main():
    st.set_page_config(layout="wide")
    st.title("TikTok Governance Issues Analysis (May 2025)")

    # 初始化 session state
    if "issue_type" not in st.session_state:
        st.session_state.issue_type = 'All'
    if "category" not in st.session_state:
        st.session_state.category = 'All'
    if "generate_summary" not in st.session_state:
        st.session_state.generate_summary = False

    # 加载数据
    df = load_data()

    # 日期过滤器
    st.sidebar.header("Date Filter")
    if 'creation_date' not in df.columns:
        st.error("No 'creation_date' column found.")
        return

    min_date = datetime(2025, 5, 1).date()
    max_date = datetime(2025, 5, 31).date()
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    # 过滤数据
    filtered_df = df[
        (df['creation_date'].dt.date >= date_range[0]) &
        (df['creation_date'].dt.date <= date_range[1])
    ]

    # 上方图表区（仅页面初次加载或日期变化时显示，不受下方筛选表单影响）
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Issue Type Distribution")
        issue_type_counts = filtered_df['issue_type'].value_counts()
        fig1 = px.pie(values=issue_type_counts.values, names=issue_type_counts.index,
                      title="Unhandled vs Mishandled Issues")
        st.plotly_chart(fig1)
    with col2:
        st.subheader("Category Distribution")
        category_counts = filtered_df['category'].value_counts()
        fig2 = px.pie(values=category_counts.values, names=category_counts.index,
                      title="Issue Categories")
        st.plotly_chart(fig2)

    st.subheader("Issue Breakdown Table by Category")
    category_table = create_category_issue_table(filtered_df)
    st.dataframe(category_table, use_container_width=True)

    # 时间趋势图
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
    # 👉 新增：基于 Raw Count 的时间流图
    col5, col6 = st.columns(2)
    with col5:
        st.markdown("**Daily Issue Type Trend (Raw Count)**")
        fig_type_count = create_daily_type_count_plot(filtered_df)
        st.plotly_chart(fig_type_count, use_container_width=True)

    with col6:
        st.markdown("**Daily Category Trend (Raw Count)**")
        fig_cat_count = create_category_raw_count_plot(filtered_df)  # 已定义，无需重命名
        st.plotly_chart(fig_cat_count, use_container_width=True)


    # 今日实时图表（全量数据，不受筛选控制）
    st.subheader("Today's Hourly Flow (Unfiltered)")
    col7, col8 = st.columns(2)

    with col7:
        st.markdown("**Hourly Issue Type Flow**")
        fig_today_flow = create_today_hourly_flow_plot(df)
        st.plotly_chart(fig_today_flow, use_container_width=True)

    with col8:
        st.markdown("**Hourly Category Flow**")
        fig_today_category_flow = create_today_hourly_category_plot(df)
        st.plotly_chart(fig_today_category_flow, use_container_width=True)

    # ✅ 下方过滤区
    st.subheader("Detailed Issue Data")
    issue_type_options = ['All'] + sorted(filtered_df['issue_type'].dropna().unique().tolist())
    category_options = ['All'] + sorted(filtered_df['category'].dropna().unique().tolist())

    with st.form(key="filter_form"):
        issue_type_selection = st.selectbox("Select Issue Type", issue_type_options,
                                            index=issue_type_options.index(st.session_state.issue_type))
        category_selection = st.selectbox("Select Category", category_options,
                                          index=category_options.index(st.session_state.category))
        apply_clicked = st.form_submit_button("Apply Filters")

        if apply_clicked:
            st.session_state.issue_type = issue_type_selection
            st.session_state.category = category_selection
            st.session_state.generate_summary = True  # ⬅️ GPT 会在后续触发

    # ✅ 应用过滤
    df_filtered_comments = filtered_df.copy()
    if st.session_state.issue_type != 'All':
        df_filtered_comments = df_filtered_comments[df_filtered_comments['issue_type'] == st.session_state.issue_type]
    if st.session_state.category != 'All':
        df_filtered_comments = df_filtered_comments[df_filtered_comments['category'] == st.session_state.category]

    # ✅ GPT 摘要仅在点击按钮后运行一次
    if st.session_state.generate_summary:
        st.session_state.generate_summary = False  # 用完即清除
        if not df_filtered_comments.empty:
            top_50 = df_filtered_comments.sort_values(by='favorite_count', ascending=False).head(50)
            summary_input = "\n\n".join([f"[{i+1}] {row['text'].strip()}" for i, row in top_50.iterrows()])
            with st.spinner("Sending to GPT..."):
                import openai
                openai.api_key = os.getenv("OPENAI_API_KEY")
                gpt_prompt = f"""
                You are a TikTok governance analyst, you are now writing an important report table based on the review you have received, as a excellent PM, you should summarize the top themes, identify the most frequently mentioned issues without loosing any details. Based on the following comments, only generate a governance detailed summary table with the following columns: Major Issue Category, Specific Detailed-Issues (with specific examples as detailed as possible), Risk Analysis with rating from 1 to 5 and potential impact. ONLY generate a detailed table, do NOT generate anything else:
                {summary_input}
                """.strip()
                response = openai.ChatCompletion.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": gpt_prompt}],
                    temperature=0.5
                )
                gpt_output = response.choices[0].message['content']
                st.markdown("### 🧠 GPT Summary")
                st.markdown(gpt_output)

    # ✅ 渲染最终表格（始终显示）
    st.subheader("Detailed Tweets")
    df_display = df_filtered_comments.copy()
    df_display['text'] = df_display['text'].apply(lambda x: x.replace('\n', ' ').strip())
    df_display = df_display.sort_values(by='creation_date', ascending=False)
    render_custom_table(df_display)






if __name__ == "__main__":
    main()
