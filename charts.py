import matplotlib.pyplot as plt
import streamlit as st
from collections import Counter
import pandas as pd
import ast
import matplotlib.cm as cm

def plot_validation_pie_chart(valid_count, invalid_count, title):
    """
    Plots a pie chart showing the proportion of valid and invalid rows.

    Parameters:
        valid_count (int): Number of valid rows.
        invalid_count (int): Number of invalid rows.
    """
    labels = ['Valid', 'Invalid']
    sizes = [valid_count, invalid_count]
    colors = ['#4CAF50', '#F44336']
    explode = (0.05, 0.05)  # Slight separation for both slices

    fig, ax = plt.subplots()
    ax.pie(sizes, explode=explode,labels=labels,colors=colors, autopct='%1.1f%%',
           shadow=True, startangle=140)
    ax.axis('equal')  # Equal aspect ratio to ensure it's a circle

    st.subheader(title)
    st.pyplot(fig)

def plot_error_counts_bar_chart(invalid_df):
    """
    Plots a bar chart showing the count of validation errors per column.

    Parameters:
        invalid_df (pd.DataFrame): DataFrame with a '__errors__' column containing stringified dictionaries.
    """
    if "__errors__" not in invalid_df.columns:
        st.warning("No '__errors__' column found in invalid data.")
        return
    
    # Counter for all column errors
    error_counter = Counter()

    for error_str in invalid_df["__errors__"]:
        try:
            error_dict = ast.literal_eval(error_str)  # Convert string to dict safely
            error_counter.update(error_dict.keys())
        except Exception as e:
            st.warning(f"Skipping invalid error format: {error_str}")

    if not error_counter:
        st.info("No column-level errors to display.")
        return
    
    # Convert to DataFrame for plotting
    error_df = pd.DataFrame.from_dict(error_counter, orient='index', columns=["Count"])
    error_df = error_df.sort_values("Count", ascending=False)

    # Generate a unique color for each bar using a colormap
    cmap = cm.get_cmap('Set3', len(error_df))
    colors = [cmap(i) for i in range(len(error_df))]

    # Plot bar chart
    fig, ax = plt.subplots()
    bars = ax.bar(error_df.index, error_df["Count"], color=colors)

    # Center count labels inside each bar
    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,   # x: center of the bar
            height / 2,                          # y: middle of the bar
            f'{int(height)}',                    # label text
            ha='center', va='center', fontsize=10, color='black', weight='bold'
        )
        
    ax.set_ylabel("Number of Errors")
    ax.set_xlabel("Column Name")
    plt.xticks(rotation=45, ha="right")
    st.subheader("📉 Column-wise Validation Error Counts")
    st.pyplot(fig)
