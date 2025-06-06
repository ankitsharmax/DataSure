import streamlit as st
from utils import load_workflows, save_workflow, apply_workflow, transform_column
from charts import plot_validation_pie_chart, plot_error_counts_bar_chart
import os
import pandas as pd

WORKFLOW_DIR = "workflows"

st.title("DataSure: Data Workflow validator")

# Sidebar Menu
st.sidebar.title("📂 Workflow Options")
menu = st.sidebar.selectbox("Choose an option", ["Create Workflow", "Run Workflow"])   
st.sidebar.markdown("Made with ❤️ by Ankit")

if menu == "Create Workflow":
    st.header("📋 Create a New Workflow")
    workflow_name = st.text_input("Workflow Name")
    col_defs = []
    
    if "columns" not in st.session_state:
        st.session_state.columns = []

    if st.button("Add Column"):
        st.session_state.columns.append({"name": "", "type": "String", "format": ""})

    for i, col in enumerate(st.session_state.columns):
        col["name"] = st.text_input(f"Column {i+1} Name", value=col["name"], key=f"name_{i}")
        col["type"] = st.selectbox(f"Column {i+1} Type", ["String", "Integer", "Double", "Date"], key=f"type_{i}")
        col["db_name"] = st.text_input(f"Column {i+1} DB Name", value=col["name"], key=f"db_name_{i}")
        col["required"] = st.checkbox(f"Required", value=col.get("required", True), key=f"required_{i}")
        if col["type"] == "Date":
            col["format"] = st.text_input(f"Date Format (e.g., yyyy-MM-dd)", value="%Y-%m-%d", key=f"format_{i}")

    if st.button("Save Workflow"):
        save_workflow(workflow_name, st.session_state.columns)
        st.success(f"Workflow '{workflow_name}' saved!")

elif menu == "Run Workflow":
    workflows = load_workflows()
    workflow_names = list(workflows.keys())

    if not workflow_names:
        st.warning("No workflows available. Please create one.")
    else:
        selected_workflow = st.selectbox("Select Workflow", workflow_names)
        uploaded_file = st.file_uploader("Upload CSV/Excel", type=["csv", "xlsx"])

        if uploaded_file:
            if uploaded_file.name.endswith("csv"):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            st.write("📄 Uploaded Data", df.head())

            valid_df, invalid_df = apply_workflow(df, workflows[selected_workflow])

            st.success("Validation Complete ✅")
            st.subheader("Valid Records")
            st.write(valid_df.head(10))

            st.subheader("Invalid Records")
            st.write(invalid_df.head(10))

            st.download_button("Download Valid Records", valid_df.to_csv(index=False), f"valid_{selected_workflow}.csv", "text/csv")
            st.download_button("Download Invalid Records", invalid_df.to_csv(index=False), f"invalid_{selected_workflow}.csv", "text/csv")

            updated_valid_df = valid_df
            updated_valid_df["__errors__"] = 'valid_data'
            combined_df = pd.concat([updated_valid_df, invalid_df], ignore_index=True)
            st.write("Combined DataFrame (Valid + Invalid)")
            st.write(combined_df.head(10))
            st.download_button("Download Overall Records", combined_df.to_csv(index=False), f"overall_{selected_workflow}.csv", "text/csv")

            valid_count = valid_df.shape[0]
            invalid_count = invalid_df.shape[0]
            plot_validation_pie_chart(valid_count, invalid_count, "📊 Data Validation Summary")
            plot_error_counts_bar_chart(invalid_df)

            schema = workflows[selected_workflow]

            # Transform each column based on the schema
            for col_def in schema:
                col_name = col_def["db_name"]
                dtype = col_def["type"]
                fmt_in = col_def.get("format", "")  # dynamic input format

                if col_name in combined_df.columns:
                    combined_df[col_name] = combined_df[col_name].apply(lambda v: transform_column(v, dtype, fmt_in))
                else:
                    st.warning(f"Column '{col_name}' not found in uploaded data.")


            st.write("🔄 Transformed Data", combined_df.head())

            transformed_data = combined_df.to_csv(index=False)
            st.download_button(
                label="Download Transformed Data as CSV",
                data=transformed_data,
                file_name=f"transformed_{selected_workflow}.csv",
                mime="text/csv"
            )

elif menu == "Transform Data":
    st.header("🛠️ Data Transformation")

    workflows = load_workflows()
    workflow_names = list(workflows.keys())

    if not workflow_names:
        st.warning("No workflows available. Please create one first.")
    else:
        selected_workflow = st.selectbox("Select Workflow", workflow_names)
        uploaded_file = st.file_uploader("Upload CSV/Excel file to transform", type=["csv", "xlsx"])

        if uploaded_file and selected_workflow:
            if uploaded_file.name.endswith(".csv"):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)

            st.write("📄 Original Data", df.head())

            schema = workflows[selected_workflow]

            # Transform each column based on the schema
            for col_def in schema:
                col_name = col_def["db_name"]
                dtype = col_def["type"]
                fmt_in = col_def.get("format", "")  # dynamic input format

                if col_name in df.columns:
                    df[col_name] = df[col_name].apply(lambda v: transform_column(v, dtype, fmt_in))
                else:
                    st.warning(f"Column '{col_name}' not found in uploaded data.")


            st.write("🔄 Transformed Data", df.head())

            csv_data = df.to_csv(index=False)
            st.download_button(
                label="Download Transformed Data as CSV",
                data=csv_data,
                file_name=f"transformed_{selected_workflow}.csv",
                mime="text/csv"
            )
