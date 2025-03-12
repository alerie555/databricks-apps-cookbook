import os
import io
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SecurableType

databricks_host = os.getenv("DATABRICKS_HOST") or os.getenv("DATABRICKS_HOSTNAME")
w = WorkspaceClient()

st.header(body="Start calculation", divider=True)
st.subheader("Provide input parameters")

st.write(
    "This is a prototype..."
)

tab1, tab2 = st.tabs(["**Start calculation**", "**Get results**"])


def check_upload_permissions(volume_name: str):
    try:
        volume = w.volumes.read(name=volume_name)
        current_user = w.current_user.me()
        grants = w.grants.get_effective(
            securable_type=SecurableType.VOLUME,
            full_name=volume.full_name,
            principal=current_user.user_name,
        )

        if not grants or not grants.privilege_assignments:
            return "Insufficient permissions: No grants found."

        for assignment in grants.privilege_assignments:
            for privilege in assignment.privileges:
                if privilege.privilege.value in ["ALL_PRIVILEGES", "WRITE_VOLUME"]:
                    return "Upload permissions validated"

        return "Insufficient permissions: Required privileges not found."
    except Exception as e:
        return f"Error: {e}"
        
        
def trigger_workflow(job_id: str, parameters: dict):
    try:
        run = w.jobs.run_now(job_id=job_id, job_parameters=parameters)
        return {
            "run_id": run.run_id,
            "state": "Triggered",
        }
    except Exception as e:
        return {"error": str(e)}


if "volume_check_success" not in st.session_state:
    st.session_state.volume_check_success = False
    
if "file_uploaded" not in st.session_state:
    st.session_state.file_uploaded = False

with tab1:
    # Description:
    description = st.text_input(
        label="Please provide a short run description or name:",
        placeholder="Description...",
    )

    # Entity: TODO: Configure drop down with long names and parse only entity ID
    entity = st.text_input(
        label="Please choose an entity:",
        placeholder="Entity",
    )

    # Scope of run: TODO: Configure drop down and parse shortened value
    scope = st.text_input(
        label="Scope of run:",
        placeholder="Scope of run",
    )

    # Fixed upload path for test
    upload_volume_path = "workspace.default.input"

    # Check permissions
    if st.button(label="Check upload permission", icon=":material/lock_reset:"):
        permission_result = check_upload_permissions(upload_volume_path.strip())
        if permission_result == "Upload permissions validated":
            st.session_state.volume_check_success = True
            st.success("Upload permissions validated", icon="✅")
        else:
            st.session_state.volume_check_success = False
            st.error(permission_result, icon="🚨")

    # If permission check successful, upload file
    if st.session_state.volume_check_success:
        uploaded_file = st.file_uploader(label="Pick a file to upload")

        if st.button(
            f"Upload file to {upload_volume_path}", icon=":material/upload_file:"
        ):
            if not upload_volume_path.strip():
                st.warning("Please specify a valid Volume path.", icon="⚠️")
            elif not uploaded_file:
                st.warning("Please pick a file to upload.", icon="⚠️")
            else:
                try:
                    file_bytes = uploaded_file.read()
                    binary_data = io.BytesIO(file_bytes)
                    file_name = uploaded_file.name
                    parts = upload_volume_path.strip().split(".")
                    catalog = parts[0]
                    schema = parts[1]
                    volume_name = parts[2]
                    volume_file_path = (
                        f"/Volumes/{catalog}/{schema}/{volume_name}/{file_name}"
                    )
                    w.files.upload(volume_file_path, binary_data, overwrite=True)
                    volume_url = f"https://{databricks_host}/explore/data/volumes/{catalog}/{schema}/{volume_name}"
                    st.success(
                        f"File '{file_name}' successfully uploaded to **{upload_volume_path}**. [Go to volume]({volume_url}).",
                        icon="✅",
                    )
                    st.session_state.file_uploaded = True
                except Exception as e:
                    st.error(f"Error uploading file: {e}", icon="🚨")

    # Once file was uploaded, trigger workflow
    
    if st.session_state.file_uploaded:
        # Job ID of Market_Flow
        job_id = 885618186794947 
        file_name = uploaded_file.name
        parameters_input = '{"entity_id": f"{entity}", "input_path": f"/Volumes/workspace/default/input/{file_name}"}'
        # parameters_input = st.text_area(
        #     label="Specify job parameters as JSON:",
        #     placeholder='{"param1": "value1", "param2": "value2"}')
    
        if st.button(label="Trigger calculation"):
            if not job_id:
                st.warning("Please specify a valid job ID.", icon="⚠️")
            elif not parameters_input.strip():
                st.warning("Please specify input parameters.", icon="⚠️")
            else:
                try:
                    parameters = eval(parameters_input.strip())
                    results = trigger_workflow(job_id, parameters)
                    if "error" in results:
                        st.error(
                            f"Error triggering workflow: {results['error']}", icon="🚨"
                        )
                    else:
                        st.success("Workflow triggered successfully", icon="✅")
                        st.json(results)
                except Exception as e:
                    st.error(f"Error parsing input parameters: {e}", icon="🚨")

    
#Once workflow was triggered, get results
with tab2:
    st.code("""
To be done...
    """)




    

