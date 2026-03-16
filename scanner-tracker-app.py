import os
import datetime as dt
from io import BytesIO

import pandas as pd
import psycopg2
import streamlit as st

# PDF library
from fpdf import FPDF  # pip install fpdf2

# -----------------------------
# Database connection
# -----------------------------
DB_HOST = st.secrets["db.ljuzgskiyasczegbudtn.supabase.co"]
DB_NAME = st.secrets["postgres"]
DB_USER = st.secrets["postgres"]
DB_PASS = st.secrets["dfazfor-wAdrub-gecco1"]
DB_PORT = st.secrets.get("db_port", 5432)


@st.cache_resource
def get_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT,
    )
    return conn


def run_query(query, params=None, fetch=True):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query, params or [])
        if fetch:
            rows = cur.fetchall()
        else:
            rows = None
    conn.commit()
    return rows


# -----------------------------
# Helper functions
# -----------------------------
def get_scanners():
    rows = run_query(
        "select id, serial_number, model, location, status, notes "
        "from scanners order by serial_number"
    )
    return pd.DataFrame(
        rows,
        columns=["id", "serial_number", "model", "location", "status", "notes"],
    )


def get_service_events(scanner_id=None):
    if scanner_id:
        rows = run_query(
            """
            select se.id, s.serial_number, se.defect, se.sent_date,
                   se.service_center, se.status, se.return_date,
                   se.cost, se.comments
            from service_events se
            join scanners s on s.id = se.scanner_id
            where se.scanner_id = %s
            order by se.sent_date desc
            """,
            [scanner_id],
        )
    else:
        rows = run_query(
            """
            select se.id, s.serial_number, se.defect, se.sent_date,
                   se.service_center, se.status, se.return_date,
                   se.cost, se.comments
            from service_events se
            join scanners s on s.id = se.scanner_id
            order by se.sent_date desc
            """
        )
    return pd.DataFrame(
        rows,
        columns=[
            "id",
            "serial_number",
            "defect",
            "sent_date",
            "service_center",
            "status",
            "return_date",
            "cost",
            "comments",
        ],
    )


def add_scanner(serial_number, model, location, status, notes):
    run_query(
        """
        insert into scanners (serial_number, model, location, status, notes)
        values (%s, %s, %s, %s, %s)
        """,
        [serial_number, model, location, status, notes],
        fetch=False,
    )


def update_scanner_status(scanner_id, status, location=None):
    if location:
        run_query(
            "update scanners set status = %s, location = %s where id = %s",
            [status, location, scanner_id],
            fetch=False,
        )
    else:
        run_query(
            "update scanners set status = %s where id = %s",
            [status, scanner_id],
            fetch=False,
        )


def add_service_event(scanner_id, defect, sent_date, service_center, status, comments):
    run_query(
        """
        insert into service_events
        (scanner_id, defect, sent_date, service_center, status, comments)
        values (%s, %s, %s, %s, %s, %s)
        """,
        [scanner_id, defect, sent_date, service_center, status, comments],
        fetch=False,
    )


def update_service_event(event_id, status, return_date=None, cost=None, comments=None):
    run_query(
        """
        update service_events
        set status = %s,
            return_date = %s,
            cost = %s,
            comments = coalesce(comments, '') || %s
        where id = %s
        """,
        [status, return_date, cost, f"\n{comments or ''}", event_id],
        fetch=False,
    )


# -----------------------------
# NEW: Store device_type in DB
# We'll add a new column to service_events: device_type
# For now, we'll store it in comments as a workaround
# or you can ALTER TABLE service_events ADD COLUMN device_type VARCHAR(50);
# -----------------------------
def add_service_event_with_device(
    scanner_id, defect, sent_date, service_center, status, comments, device_type
):
    """Enhanced version that stores device_type in comments for now."""
    enhanced_comments = f"[Device Type: {device_type}]\n{comments or ''}"
    run_query(
        """
        insert into service_events
        (scanner_id, defect, sent_date, service_center, status, comments)
        values (%s, %s, %s, %s, %s, %s)
        """,
        [scanner_id, defect, sent_date, service_center, status, enhanced_comments],
        fetch=False,
    )


def extract_device_type_from_comments(comments: str) -> str:
    """Extract device type from comments if stored there."""
    if not comments:
        return "Unknown"
    if "[Device Type:" in comments:
        start = comments.find("[Device Type:") + len("[Device Type:")
        end = comments.find("]", start)
        if end > start:
            return comments[start:end].strip()
    return "Unknown"


# -----------------------------
# PDF helpers
# -----------------------------
def create_verbal_process_pdf(
    person_name: str,
    serial_number: str,
    device_type: str,
    model: str,
    location: str,
    process_date: dt.date,
    service_center: str,
    defect: str,
    service_event_id: int | None = None,
) -> bytes:
    """
    Generate a verbal process PDF when sending a device to service.
    """
    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    # Title
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, "Verbal Process - Device Sent to Service", ln=1, align="C")
    pdf.ln(5)

    pdf.set_font("Arial", size=11)

    # Basic info
    pdf.cell(60, 8, "Name:", 0, 0)
    pdf.cell(0, 8, person_name or "-", ln=1)

    pdf.cell(60, 8, "Date:", 0, 0)
    pdf.cell(0, 8, process_date.strftime("%Y-%m-%d"), ln=1)

    if service_event_id is not None:
        pdf.cell(60, 8, "Service Event ID:", 0, 0)
        pdf.cell(0, 8, str(service_event_id), ln=1)

    pdf.cell(60, 8, "Serial Number:", 0, 0)
    pdf.cell(0, 8, serial_number, ln=1)

    pdf.cell(60, 8, "Device Type:", 0, 0)
    pdf.cell(0, 8, device_type, ln=1)

    pdf.cell(60, 8, "Model:", 0, 0)
    pdf.cell(0, 8, model, ln=1)

    pdf.cell(60, 8, "Location:", 0, 0)
    pdf.cell(0, 8, location, ln=1)

    pdf.cell(60, 8, "Service Center:", 0, 0)
    pdf.cell(0, 8, service_center, ln=1)

    pdf.ln(5)
    pdf.multi_cell(0, 8, f"Defect / Description: {defect or '-'}")

    # Footer
    pdf.ln(15)
    pdf.cell(0, 8, "Authorized by: __________________________", ln=1)
    pdf.ln(5)
    pdf.cell(0, 8, "Signature: __________________________", ln=1)

    # Return bytes
    pdf_bytes = pdf.output(dest="S").encode("latin1")
    return pdf_bytes


def create_return_receipt_pdf(
    person_name: str,
    serial_number: str,
    device_type: str,
    model: str,
    location: str,
    return_date: dt.date,
    service_center: str,
    defect: str,
    service_event_id: int | None = None,
    cost: float | None = None,
    comments: str = "",
) -> bytes:
    """
    Generate a return receipt PDF when a device is returned from service.
    """
    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    # Title
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, "Return Receipt - Device Returned from Service", ln=1, align="C")
    pdf.ln(5)

    pdf.set_font("Arial", size=11)

    # Basic info
    pdf.cell(60, 8, "Received by:", 0, 0)
    pdf.cell(0, 8, person_name or "-", ln=1)

    pdf.cell(60, 8, "Return Date:", 0, 0)
    pdf.cell(0, 8, return_date.strftime("%Y-%m-%d"), ln=1)

    if service_event_id is not None:
        pdf.cell(60, 8, "Service Event ID:", 0, 0)
        pdf.cell(0, 8, str(service_event_id), ln=1)

    pdf.cell(60, 8, "Serial Number:", 0, 0)
    pdf.cell(0, 8, serial_number, ln=1)

    pdf.cell(60, 8, "Device Type:", 0, 0)
    pdf.cell(0, 8, device_type, ln=1)

    pdf.cell(60, 8, "Model:", 0, 0)
    pdf.cell(0, 8, model, ln=1)

    pdf.cell(60, 8, "Current Location:", 0, 0)
    pdf.cell(0, 8, location, ln=1)

    pdf.cell(60, 8, "Service Center:", 0, 0)
    pdf.cell(0, 8, service_center, ln=1)

    if cost is not None and cost > 0:
        pdf.cell(60, 8, "Service Cost:", 0, 0)
        pdf.cell(0, 8, f"${cost:.2f}", ln=1)

    pdf.ln(5)
    pdf.multi_cell(0, 8, f"Original Defect: {defect or '-'}")

    if comments:
        pdf.ln(3)
        # Clean device type tag from comments if present
        clean_comments = comments
        if "[Device Type:" in clean_comments:
            idx = clean_comments.find("]")
            if idx > -1:
                clean_comments = clean_comments[idx + 1:].strip()
        pdf.multi_cell(0, 8, f"Service Notes: {clean_comments}")

    # Footer
    pdf.ln(15)
    pdf.cell(0, 8, "Device condition verified by: __________________________", ln=1)
    pdf.ln(5)
    pdf.cell(0, 8, "Signature: __________________________", ln=1)

    # Return bytes
    pdf_bytes = pdf.output(dest="S").encode("latin1")
    return pdf_bytes


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Scanner Service Tracker", layout="wide")
st.title("Scanner Service Tracker")


tab_dashboard, tab_scanners, tab_service = st.tabs(
    ["Dashboard", "Scanners", "Service events"]
)


# -----------------------------
# Dashboard tab
# -----------------------------
with tab_dashboard:
    st.subheader("Current stock overview")

    df_scanners = get_scanners()
    if df_scanners.empty:
        st.info("No scanners in database yet.")
    else:
        status_counts = df_scanners["status"].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        col1, col2 = st.columns(2)

        with col1:
            st.metric(
                "Total scanners",
                int(len(df_scanners)),
            )
            st.write("By status:")
            st.dataframe(status_counts, use_container_width=True)

        with col2:
            st.write("Scanners currently in service:")
            in_service = df_scanners[df_scanners["status"] == "in_service"]
            st.dataframe(
                in_service[["serial_number", "model", "location", "notes"]],
                use_container_width=True,
            )

    st.subheader("Service performance")

    df_events = get_service_events()
    if df_events.empty:
        st.info("No service events recorded yet.")
    else:
        df_events["service_time_days"] = None
        mask = df_events["return_date"].notna()
        df_events.loc[mask, "service_time_days"] = (
            df_events.loc[mask, "return_date"] - df_events.loc[mask, "sent_date"]
        ).dt.days

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total service events", int(len(df_events)))
        with col2:
            avg = (
                df_events["service_time_days"].dropna().mean()
                if df_events["service_time_days"].notna().any()
                else None
            )
            st.metric(
                "Average service time (days)",
                f"{avg:.1f}" if avg is not None else "n/a",
            )
        with col3:
            open_count = (df_events["status"] != "returned").sum()
            st.metric("Open service cases", int(open_count))

        st.write("Service events by defect:")
        defect_counts = df_events["defect"].value_counts().reset_index()
        defect_counts.columns = ["defect", "count"]
        st.dataframe(defect_counts, use_container_width=True)


# -----------------------------
# Scanners tab
# -----------------------------
with tab_scanners:
    st.subheader("Add new scanner")

    with st.form("add_scanner_form"):
        col1, col2 = st.columns(2)
        with col1:
            serial_number = st.text_input("Serial number")
            model = st.text_input("Model")
        with col2:
            location = st.text_input("Location", value="Warehouse")
            status = st.selectbox(
                "Status", ["in_stock", "in_service", "defective", "retired"]
            )
        notes = st.text_area("Notes", height=80)
        submitted = st.form_submit_button("Add scanner")

    if submitted:
        if not serial_number:
            st.error("Serial number is required.")
        else:
            add_scanner(serial_number, model, location, status, notes)
            st.success(f"Scanner {serial_number} added.")
            st.rerun()

    st.subheader("All scanners")
    df_scanners = get_scanners()
    if not df_scanners.empty:
        st.dataframe(
            df_scanners.drop(columns=["id"]),
            use_container_width=True,
        )

        st.subheader("Quick status update")
        scanners_map = {
            f"{row.serial_number} ({row.model})": row.id
            for row in df_scanners.itertuples()
        }
        selected = st.selectbox(
            "Select scanner to update", ["-"] + list(scanners_map.keys())
        )
        if selected != "-":
            new_status = st.selectbox(
                "New status",
                ["in_stock", "in_service", "defective", "retired"],
            )
            new_location = st.text_input(
                "New location (optional, leave blank to keep current)"
            )
            if st.button("Update status"):
                scanner_id = scanners_map[selected]
                update_scanner_status(
                    scanner_id,
                    new_status,
                    new_location or None,
                )
                st.success("Status updated.")
                st.rerun()


# -----------------------------
# Service events tab
# -----------------------------
with tab_service:
    st.subheader("Register scanner sent to service")

    df_scanners = get_scanners()
    if df_scanners.empty:
        st.info("Add scanners first.")
    else:
        scanners_map = {
            f"{row.serial_number} ({row.model})": row.id
            for row in df_scanners.itertuples()
        }
        with st.form("send_to_service_form"):
            scanner_label = st.selectbox("Scanner", list(scanners_map.keys()))
            defect = st.text_input("Defect / error description")
            sent_date = st.date_input("Sent date", value=dt.date.today())
            service_center = st.text_input("Service center", value="Default center")
            status = st.selectbox("Initial status", ["sent", "in_progress", "returned"])
            # NEW: device type + name for verbal process
            device_type = st.selectbox("Device type", ["Zebra", "Honeywell"])
            person_name = st.text_input("Name for verbal process (person sending)")

            comments = st.text_area("Comments", height=80)
            submitted_service = st.form_submit_button("Register service event")

        if submitted_service:
            if not defect:
                st.error("Defect description is required.")
            else:
                scanner_id = scanners_map[scanner_label]
                add_service_event_with_device(
                    scanner_id,
                    defect,
                    sent_date,
                    service_center,
                    status,
                    comments,
                    device_type,
                )
                update_scanner_status(scanner_id, "in_service")
                st.success("Service event added and scanner marked as in_service.")

                # Fetch scanner details for PDF
                scanner_row = df_scanners[df_scanners["id"] == scanner_id].iloc[0]
                serial_number = scanner_row["serial_number"]
                model = scanner_row["model"]
                location = scanner_row["location"]

                # Get last service event id for this scanner
                df_ev_scanner = get_service_events(scanner_id)
                if not df_ev_scanner.empty:
                    service_event_id = int(df_ev_scanner.iloc[0]["id"])
                else:
                    service_event_id = None

                pdf_bytes = create_verbal_process_pdf(
                    person_name=person_name or "",
                    serial_number=serial_number,
                    device_type=device_type,
                    model=model,
                    location=location,
                    process_date=sent_date,
                    service_center=service_center,
                    defect=defect,
                    service_event_id=service_event_id,
                )
                pdf_filename = f"verbal_process_{serial_number}_{sent_date}.pdf"

                st.download_button(
                    label="📄 Download Verbal Process PDF",
                    data=pdf_bytes,
                    file_name=pdf_filename,
                    mime="application/pdf",
                )

    st.subheader("Update open service cases")

    df_events = get_service_events()
    if df_events.empty:
        st.info("No service events yet.")
    else:
        open_events = df_events[df_events["status"] != "returned"]
        if open_events.empty:
            st.info("No open cases.")
        else:
            events_map = {
                f"{row.id} - {row.serial_number} - {row.defect} ({row.status})": row.id
                for row in open_events.itertuples()
            }
            selected_event_label = st.selectbox(
                "Select open event", list(events_map.keys())
            )
            event_id = events_map[selected_event_label]
            
            # Get current event details
            current_event = open_events[open_events["id"] == event_id].iloc[0]
            
            new_status = st.selectbox(
                "New status", ["sent", "in_progress", "returned", "scrapped"]
            )
            return_date = st.date_input(
                "Return date (if returned)", value=dt.date.today()
            )
            cost = st.number_input("Service cost", min_value=0.0, value=0.0)
            extra_comments = st.text_area("Additional comments", height=60)
            
            # NEW: Name for return receipt
            return_person_name = st.text_input(
                "Name (person receiving device back)", 
                key="return_person_name"
            )

            if st.button("Update service event"):
                rd = return_date if new_status == "returned" else None
                c = cost if cost > 0 else None
                update_service_event(
                    event_id,
                    new_status,
                    rd,
                    c,
                    extra_comments,
                )

                # If returned, mark scanner as in_stock and generate return receipt
                if new_status == "returned":
                    scanner_row = run_query(
                        "select scanner_id from service_events where id = %s",
                        [event_id],
                    )[0]
                    scanner_id = scanner_row[0]
                    update_scanner_status(scanner_id, "in_stock")

                    # Fetch scanner details for return receipt PDF
                    df_scanners_fresh = get_scanners()
                    scanner_details = df_scanners_fresh[
                        df_scanners_fresh["id"] == scanner_id
                    ].iloc[0]

                    # Extract device type from comments
                    device_type = extract_device_type_from_comments(
                        current_event["comments"]
                    )

                    # Generate return receipt PDF
                    return_pdf_bytes = create_return_receipt_pdf(
                        person_name=return_person_name or "",
                        serial_number=scanner_details["serial_number"],
                        device_type=device_type,
                        model=scanner_details["model"],
                        location=scanner_details["location"],
                        return_date=return_date,
                        service_center=current_event["service_center"],
                        defect=current_event["defect"],
                        service_event_id=event_id,
                        cost=c,
                        comments=current_event["comments"],
                    )
                    return_pdf_filename = (
                        f"return_receipt_{scanner_details['serial_number']}_"
                        f"{return_date}.pdf"
                    )

                    st.success("Service event updated and scanner returned to stock.")
                    
                    st.download_button(
                        label="📄 Download Return Receipt PDF",
                        data=return_pdf_bytes,
                        file_name=return_pdf_filename,
                        mime="application/pdf",
                        key="download_return_receipt",
                    )
                else:
                    st.success("Service event updated.")

    st.subheader("All service events")
    df_all = get_service_events()
    if not df_all.empty:
        st.dataframe(df_all, use_container_width=True)