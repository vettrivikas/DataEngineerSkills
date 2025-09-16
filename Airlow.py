raw_dt = dag_run_conf.get("process_com_dt")
    if raw_dt:
        try:
            dt = datetime.fromisoformat(raw_dt)
        except ValueError:
            dt = datetime.strptime(raw_dt, "%Y-%m-%d %H:%M:%S.%f%z")
        normalized_dt = dt.replace(tzinfo=None).isoformat()
    else:
        normalized_dt = datetime.now().isoformat()

    message = {
        "event_id": "693217fc-3ce2-4b12-b70e-148504e0f7b7",
        "event": event_id,
        "database": dag_run_conf.get("database", ""),
        "process_name": process_name,
        "data_date": dag_run_conf.get("data_date", datetime.now().strftime("%Y%m%d")),
        "process_com_dt": normalized_dt,  # ⬅️ directly applied here
        "audit_id": datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3],
    }
