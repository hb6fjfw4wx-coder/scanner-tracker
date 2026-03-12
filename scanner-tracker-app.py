import os
import datetime as dt

import pandas as pd
import psycopg2
import streamlit as st

# -----------------------------
# Database connection
# -----------------------------
DB_HOST = st.secrets["db_host"]
DB_NAME = st.secrets["db_name"]
DB_USER = st.secrets["db_user"]
DB_PASS = st.secrets["db_pass"]
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
                in_service[
                    ["serial_number", "model", "location", "notes"]
                ],
                use_container_width=True,
            )

    st.subheader("Service performance")

    df_events = get_service_events()
    if df_events.empty:
        st.info("No service events recorded yet.")
    else:
        # Compute service time in days where return_date is set
        df_events["service_time_days"] = None
        mask = df_events["return_date"].notna()
        df_events.loc[mask, "service_time_days"] = (
            df_events.loc[mask, "return_date"] - df_events.loc[mask, "sent_date"]
        ).dt.days

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                "Total service events", int(len(df_events))
            )
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
        defect_counts = (
            df_events["defect"].value_counts().reset_index()
        )
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
                st.success("Status updated. Refresh to see changes.")

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
            scanner_label = st.selectbox(
                "Scanner", list(scanners_map.keys())
            )
            defect = st.text_input("Defect / error description")
            sent_date = st.date_input("Sent date", value=dt.date.today())
            service_center = st.text_input("Service center", value="Default center")
            status = st.selectbox(
                "Initial status", ["sent", "in_progress", "returned"]
            )
            comments = st.text_area("Comments", height=80)
            submitted_service = st.form_submit_button("Register service event")

        if submitted_service:
            if not defect:
                st.error("Defect description is required.")
            else:
                scanner_id = scanners_map[scanner_label]
                add_service_event(
                    scanner_id,
                    defect,
                    sent_date,
                    service_center,
                    status,
                    comments,
                )
                # Put scanner in service status
                update_scanner_status(scanner_id, "in_service")
                st.success("Service event added and scanner marked as in_service.")

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
            new_status = st.selectbox(
                "New status", ["sent", "in_progress", "returned", "scrapped"]
            )
            return_date = st.date_input(
                "Return date (if returned)", value=dt.date.today()
            )
            cost = st.number_input("Service cost", min_value=0.0, value=0.0)
            extra_comments = st.text_area("Additional comments", height=60)

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

                # If returned, also mark scanner as in_stock
                if new_status == "returned":
                    # Find scanner id
                    scanner_row = run_query(
                        "select scanner_id from service_events where id = %s",
                        [event_id],
                    )[0]
                    scanner_id = scanner_row[0]
                    update_scanner_status(scanner_id, "in_stock")

                st.success("Service event updated.")
    
    st.subheader("All service events")
    df_all = get_service_events()
    if not df_all.empty:
        st.dataframe(df_all, use_container_width=True)

