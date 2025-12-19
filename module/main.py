import pandas as pd

from .connections.pg_connect import pg_db_connect
from .connections.ssms_connect import ssms_connect
from .helpers.ssms_to_dict import ssms_to_dict

import psycopg2

conn, cur, cur_dict = pg_db_connect()
ssms_conn, ssms_cur = ssms_connect()

sub_company_number = '20'
project_number = '025020-'

def validate_and_sanitize_input_data(sub_co_number: int, proj_number: str):
    """Sanitizes values passed by user so that errors do not occur when
        querying the databases. sub_co_number must be an integer.
        project_number is organized 2 different ways:
            ssms/vista - example project number is " 024013-"
            postgres - example project number is "024013"
        Project number can be passed either way and this will prepare it
        for querying both databases.
    """
    sub_company_number = None
    ssms_project_number = None
    pg_project_number = None

    # check and clean sub_company_number
    if type(sub_co_number) == int:
        sub_company_number = sub_co_number
    elif type(sub_co_number) == str:
        try:
            sub_company_number = int(sub_co_number)
        except:
            raise ValueError("sub_company_number passed as string and is not an int.")
    else:
        raise ValueError('sub_company_number must be int-like')
    
    # check proj_number type
    if type(proj_number) != str:
        raise ValueError('project_number must be a string')

    project_number = proj_number.strip()

    if project_number[-1] == '-':
        ssms_project_number = project_number
        pg_project_number = project_number.strip('-')
    else:
        ssms_project_number = f"{project_number}-"
        pg_project_number = project_number

    return sub_company_number, ssms_project_number, pg_project_number

def run_budget_cleanup(sub_company_number: int, project_number: str):
    """Runs a budget cleanup for a specific project in postgres. 
        Use if there is a budget discrepancy during an update.
        Matches key_ids from postgres with KeyIDs from Vista to find
        key_ids existing in postgres that no longer exist in Vista.
    """

    sub_company_number, ssms_project_number, pg_project_number = validate_and_sanitize_input_data(sub_co_number=sub_company_number, proj_number=project_number)

    # print(f"sub_company_number, type: {type(sub_company_number)}, value: {sub_company_number}")
    # print(f"ssms_project_number, type: {type(ssms_project_number)}, value: {ssms_project_number}")
    # print(f"pg_project_number, type: {type(pg_project_number)}, value: {pg_project_number}")

    def get_pg_budget_data(cur_dict: any, sub_company_number: int, project_number: str):
        """
            Returns all budget transactions in postgres for a specific project.
            cur_dict should be a RealDictCursor from psycopg2 so that resulting rows
            are dictionaries.
        """
        pg_sql = """
                SELECT
                    a.id,
                    i.parent_company_id,
                    j.name parent_company_name,
                    h.sub_company_id,
                    i.number sub_company_number,
                    i.name sub_company_name,
                    g.department_id,
                    h.number department_number,
                    h.name department_name,
                    f.project_id,
                    g.number project_number,
                    g.name project_name,
                    c.bid_item_id,
                    f.number bid_item_number,
                    f.name bid_item_name,
                    a.cost_code_cost_type_id,
                    b.cost_code_id,
                    c.number cost_code_number,
                    c.name cost_code_name,
                    b.cost_type_id,
                    d.number cost_type,
                    d.name cost_type_letters,
                    b.uom_id,
                    e.letters uom,
                    a.mth,
                    a.type,
                    a.actual_date,
                    a.posted_date,
                    a.quantity,
                    a.hours,
                    a.amount,
                    a.key_id,
                    a.source
                FROM jc_budgets a
                LEFT JOIN jc_cost_code_cost_types b ON a.cost_code_cost_type_id = b.id
                LEFT JOIN jc_cost_codes c ON b.cost_code_id = c.id
                LEFT JOIN co_cost_types d ON b.cost_type_id = d.id
                LEFT JOIN co_uoms e ON b.uom_id = e.id
                LEFT JOIN jb_bid_items f ON c.bid_item_id = f.id
                LEFT JOIN co_projects g ON f.project_id = g.id
                LEFT JOIN co_departments h ON g.department_id = h.id
                LEFT JOIN co_sub_companies i ON h.sub_company_id = i.id
                LEFT JOIN co_parent_companies j ON i.parent_company_id = j.id
                WHERE i.number = %s
                AND g.number = %s
                """
        try:
            cur_dict.execute(pg_sql, (sub_company_number, project_number))
            results = cur_dict.fetchall()
            print(f"Rows found in PG: {len(results)}")
            return results
        except Exception as e:
            raise ValueError(f"Error getting data from postgres: {e}")


    def get_ssms_budget_data(ssms_cur, sub_company_number, project_number):
        """
            Gets SSMS data and organizes each row into a dict.
        """
        try:
            ssms_cur.execute("""
                            SELECT 
                                a.JCCo,
                                a.Mth,
                                a.CostTrans,
                                LTRIM(RTRIM(a.Job)) Job,	
                                LTRIM(RTRIM(a.Phase)) Phase,
                                a.CostType,
                                a.PostedDate,
                                a.ActualDate,
                                a.JCTransType,
                                a.Source,
                                a.Description,
                                a.EstUnits,
                                a.EstHours,
                                a.EstCost,
                                a.KeyID
                            FROM JCCD a
                            WHERE a.JCCo = ? 
                            AND LTRIM(RTRIM(a.Job)) = ?
                            AND JCTransType IN ('CO', 'OE')
                            --AND (a.EstUnits <> 0 OR a.EstHours <> 0 OR a.EstCost <> 0)
                            """, (sub_company_number, project_number))

            results = ssms_cur.fetchall()

            print(f"Rows found in SSMS: {len(results)}")
            ssms_dict_rows = ssms_to_dict(ssms_cur, results)
            for row in ssms_dict_rows:
                row['Job'] = row['Job'].strip('-')
            return ssms_dict_rows
        except Exception as e:
            raise ValueError(f"Error getting data from SSMS: {e}")
        
    pg_rows = get_pg_budget_data(cur_dict=cur_dict, sub_company_number=sub_company_number, project_number=pg_project_number)

    ssms_rows = get_ssms_budget_data(ssms_cur=ssms_cur, sub_company_number=sub_company_number, project_number=ssms_project_number)

    pg_df = pd.DataFrame(pg_rows)

    ssms_df = pd.DataFrame(ssms_rows)

    # pg_columns = pg_df.columns
    # ssms_columns = ssms_df.columns

    # print('pg_df columns:', pg_columns)
    # print('ssms_df columns:', ssms_columns)

    # merges both dfs but always returns all pg_df rows
    merged_df = pg_df.merge(
        ssms_df,
        how="left",
        left_on="key_id",
        right_on="KeyID",
        indicator=True
    )

    unmatched_df = merged_df[merged_df["_merge"] == 'left_only'].copy() # returns only rows that did not have a matching ssms row

    cols = ["quantity", "hours", "amount"]
    unmatched_df_filtered = unmatched_df.loc[unmatched_df[cols].fillna(0).ne(0).any(axis=1)] # filters out any rows where quantity, hours, and amount from pg are all 0
    # print(unmatched_df_filtered.head())
    
    key_id_list = unmatched_df_filtered['key_id'].dropna().unique().tolist() # returns a python list of key_ids existing in postgres and not ssms
    print(f"key_ids that need removed from postgres jc_budgets: {key_id_list}")
    
    amount_summary = (unmatched_df_filtered
                    .groupby(['project_number', 'cost_type_letters', 'source'], as_index=False)
                        .agg(total_amount=("amount", "sum"))
    )
    summary_records = amount_summary.to_dict(orient='records')

    def delete_key_ids(cur, key_id_list):
        for key_id in key_id_list:
            print(f"Deleting key_id: {key_id} from jc_budgets...")
            cur.execute("""DELETE FROM jc_budgets WHERE key_id = %s""", (key_id,))

    delete_key_id_input = input("Do you want to delete the key_ids (Y/N)?: ")

    if delete_key_id_input.lower() in ['y', 'yes']:
        print('Deleting key_ids from jc_budgets...')
        delete_key_ids(cur=cur, key_id_list=key_id_list)
    else:
        print("Not deleteing key_ids from jc_budgets. Done.")