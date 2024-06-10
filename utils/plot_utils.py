import pandas as pd
import sys
from matplotlib import pyplot as plt
import seaborn as sns
import plotly
import plotly.graph_objects as go


def bar_bar(input_df, col1, col2):
    fig = go.Figure()
    fig.add_trace(go.Bar(x=input_df['time'], y=input_df[col1], marker_color='crimson', name=col1))
    fig.add_trace(go.Bar(x=input_df['time'], y=input_df[col2], marker_color='black', name=col2, yaxis='y2'))
    fig.update_layout(
        legend=dict(orientation='h'),  
        barmode='group',
        yaxis=dict(
            title={'text': col1}, 
            side='left'
        ), 
        yaxis2=dict(
            title={'text': col2},
            side='right', 
            overlaying='y', 
            tickmode='sync', 
            range=[0, input_df[col2].max()*1.5]
        )
    )
    return fig


def scatters(input_df, cols: list):
    fig = go.Figure()
    for col in cols:
        fig.add_trace(go.Scatter(x=input_df['time'], y=input_df[col], name=col))
    fig.update_layout(
    # legend=dict(orientation='h'), 
    yaxis=dict(
        title={'text': 'growth percentage (%)'}, 
        side='left', 
        )
    )
        
    return fig


def bar_and_scatter(input_df, col1, col2):
    fig = go.Figure()
    fig.add_trace(go.Bar(x=input_df['time'], y=input_df['total_size'], marker_color='crimson', name=col1, base=0))
    fig.add_trace(go.Scatter(x=input_df['time'], y=input_df['n_live_tup'], marker_color='black', name=col2, yaxis='y2'))

    fig.update_layout(
        legend=dict(orientation='h'), 
        yaxis=dict(
            title={'text': col1}, 
            side='left', 
        ), 
        yaxis2=dict(
            title={'text': col2},
            side='right', 
            overlaying='y', 
            tickmode='sync', 
            range=[abs(input_df[col2].min()) / 2, input_df[col2].max()]
        )
    )
    return fig


def bar_bar_and_scatter(input_df, col11, col12, col2):
    fig = go.Figure()
    base1 = input_df[col12].min() / 2
    fig.add_trace(go.Bar(x=input_df['time'], y=input_df[col11], marker_color='crimson', name=col11))
    fig.add_trace(go.Bar(x=input_df['time'], y=input_df[col12], marker_color='turquoise', name=col12))
    fig.add_trace(go.Scatter(x=input_df['time'], y=input_df[col2], marker_color='black', name=col2, yaxis='y2'))

    fig.update_layout(
        legend=dict(orientation='h'), 
        yaxis=dict(
            title={'text': 'size'}, 
            side='left', 
            range=[0, input_df[col11].max()*1.1]
        ), 
        yaxis2=dict(
            title={'text': col2},
            side='right', 
            overlaying='y', 
            tickmode='sync', 
            range=[0, input_df[col2].max()*1.1]
        )
    )
    return fig
