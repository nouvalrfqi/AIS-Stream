def derive_status(nav_status, sog_knots) -> str:
    sog = sog_knots or 0
    if nav_status == 1:
        return "ANCHORED"
    if nav_status == 5:
        return "MOORED"
    if nav_status == 0 and sog == 0:
        return "STOPPED"
    if nav_status in (0, 8) and sog > 0:
        return "UNDERWAY"
    return "UNKNOWN"