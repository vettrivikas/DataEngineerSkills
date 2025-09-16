def format_message_for_email(message: dict) -> str:
    return (
        "{\n"
        f"'event_id'       : '{message.get('event_id')}'\n"
        f"'event'          : '{message.get('event')}'\n"
        f"'database'       : '{message.get('database', '')}'\n"
        f"'process_name'   : '{message.get('process_name')}'\n"
        f"'data_date'      : '{message.get('data_date')}'\n"
        f"'process_com_dt' : '{message.get('process_com_dt')}'\n"
        f"'audit_id'       : '{message.get('audit_id')}'\n"
        "}"
    )
