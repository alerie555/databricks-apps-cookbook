import os
import io
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SecurableType

databricks_host = os.getenv("DATABRICKS_HOST") or os.getenv("DATABRICKS_HOSTNAME")
w = WorkspaceClient()

st.header(body="Run status", divider=True)
st.subheader("Run status")

st.write(
    "This recipe uploads a file to a [Unity Catalog Volume](https://docs.databricks.com/en/volumes/index.html)."
)

tab1, tab2 = st.tabs(["**Run status**", "**Run logs**"])


with tab1:
    st.code("""
To be done...
    """)

with tab2:
    st.code("""
To be done...
    """)