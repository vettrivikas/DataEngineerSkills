def format_message_for_email(message: dict) -> str:
    return (
        "{\n"
        f"'Event ID'     : '{message.get('event_id')}'\n"
        f"'Event'        : '{message.get('event')}'\n"
        f"'Process Name' : '{message.get('process_name')}'\n"
        f"'Data Date'    : '{message.get('data_date')}'\n"
        f"'Completed At' : '{message.get('process_com_dt')}'\n"
        f"'Audit ID'     : '{message.get('audit_id')}'\n"
        "}"
    )
