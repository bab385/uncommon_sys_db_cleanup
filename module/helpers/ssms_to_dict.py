def ssms_to_dict(cur, query_results):
    columns = [column[0] for column in cur.description]

    results = []

    for row in query_results:
        results.append(dict(zip(columns, row)))

    return results