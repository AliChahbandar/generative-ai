import time

from google.cloud import bigquery
import streamlit as st
from vertexai.generative_models import FunctionDeclaration, GenerativeModel, Part, Tool

SQL_Examples = [
  {
    "Question": "How many members do we currently have for Cigna?",
    "SQL": "SELECT COUNT(M.DTL_CIM) FROM `ACA.MEMBERS` M, `ACA.CARRIERCODE_ACA_REF` CC, `ACA.CASE_MASTER_DTL` CM WHERE M.DTL_CIM = CM.DTL_CIM AND CM.CARRIER_CODE = CC.CARRIER_CODE AND M.TERM_DATE IS NULL AND CC.CARRIER_NAME = 'Cigna'",
  },
  {
    "Question": "How many members terminated with Molina during the past year?",
    "SQL": "SELECT COUNT(M.DTL_CIM) FROM `ACA.MEMBERS` M, `ACA.CARRIERCODE_ACA_REF` CC, `ACA.CASE_MASTER_DTL` CM WHERE M.DTL_CIM = CM.DTL_CIM AND CM.CARRIER_CODE = CC.CARRIER_CODE AND EXTRACT(YEAR FROM DATE (M.TERM_DATE)) = 2023 AND CC.CARRIER_NAME = 'Molina'",
  },
  {
    "Question": "How many members terminations were processed with Molina during the past year?",
    "SQL": "SELECT COUNT(M.DTL_CIM) FROM `ACA.MEMBERS` M, `ACA.CARRIERCODE_ACA_REF` CC, `ACA.CASE_MASTER_DTL` CM WHERE M.DTL_CIM = CM.DTL_CIM AND CM.CARRIER_CODE = CC.CARRIER_CODE AND EXTRACT(YEAR FROM DATE (M.TERM_PROCESSED_DATE)) = 2023 AND CC.CARRIER_NAME = 'Molina'",
  },
  {
    "Question": "What is the trend of our overall membership for the past 3 years?",
    "SQL": "SELECT COUNT(M.DTL_CIM) AS MEMBERSHIP, '2023' AS YEAR FROM `ACA.MEMBERS` M, `ACA.CARRIERCODE_ACA_REF` CC, `ACA.CASE_MASTER_DTL` CM WHERE M.DTL_CIM = CM.DTL_CIM AND CM.CARRIER_CODE = CC.CARRIER_CODE AND CC.CARRIER_NAME = 'Christus' AND M.EFFECTIVE_DATE <= '2023-12-31' AND ( M.TERM_DATE IS NULL OR ( M.TERM_DATE >= '2023-01-01' AND M.TERM_DATE > M.EFFECTIVE_DATE ) ) UNION ALL SELECT COUNT(M.DTL_CIM) AS MEMBERSHIP, '2022' AS YEAR FROM `ACA.MEMBERS` M, `ACA.CARRIERCODE_ACA_REF` CC, `ACA.CASE_MASTER_DTL` CM WHERE M.DTL_CIM = CM.DTL_CIM AND CM.CARRIER_CODE = CC.CARRIER_CODE AND CC.CARRIER_NAME = 'Christus' AND M.EFFECTIVE_DATE <= '2022-12-31' AND ( M.TERM_DATE IS NULL OR ( M.TERM_DATE >= '2022-01-01' AND M.TERM_DATE > M.EFFECTIVE_DATE ) ) UNION ALL SELECT COUNT(M.DTL_CIM) AS MEMBERSHIP, '2021' AS YEAR FROM `ACA.MEMBERS` M, `ACA.CARRIERCODE_ACA_REF` CC, `ACA.CASE_MASTER_DTL` CM WHERE M.DTL_CIM = CM.DTL_CIM AND CM.CARRIER_CODE = CC.CARRIER_CODE AND CC.CARRIER_NAME = 'Christus' AND M.EFFECTIVE_DATE = '2021-12-31' AND ( M.TERM_DATE IS NULL OR ( M.TERM_DATE >= '2021-01-01' AND M.TERM_DATE > M.EFFECTIVE_DATE ) ) Order by YEAR DESC",
  },
  {
    "Question": "What is the enrollment split for HCSC between medical and dental?",
    "SQL": "SELECT COUNT(*) As MEMBERS, P.PRODUCT_TYPE FROM `ACA.MEMBERS` M, `ACA.PLAN_ID` P, `ACA.CARRIERCODE_ACA_REF` CC WHERE M.TERM_DATE IS NULL AND M.DTL_CIM = P.DTL_CIM AND P.CARRIER_CODE = CC.CARRIER_CODE AND CC.CARRIER_NAME = 'HCSC' GROUP BY P.PRODUCT_TYPE",
  },
  {
    "Question": "What is the average APTC for our members in Florida?",
    "SQL": "SELECT AVG(A.APTC_AMOUNT) FROM `ACA.APTC` A, `ACA.CASE_MASTER_DTL` CMD WHERE CMD.ACTIVE_CODE = 'CM' AND A.STATUS = 'C' AND A.DTL_CIM = CMD.DTL_CIM AND CMD.STATE = 'FL'",
  },
  {
    "Question": "How many discrepancies are outstanding per carrier per HIOs?",
    "SQL": "SELECT CARRIER_NAME, HIOS, COUNT(HIOS) AS OutStandingDiscCount FROM RAW.RECONInventory WHERE DISC_STATUS_NAME != 'Resolved' GROUP BY CARRIER_NAME, HIOS",
  },
  {
    "Question": "How many HICS cases does each HIOS have per Carrier?",
    "SQL": "SELECT CW.HIOS, RCI.CARRIER_NAME, COUNT(CW.HIOS) FROM `hps-aca-recon-dev-a210.HICS.CaseWorks` CW LEFT JOIN `RAW.RECONInventory` RCI ON CW.HIOS = CAST(RCI.HIOS AS STRING) AND RCI.DTL_CASE = CW.ILB_Number GROUP BY CW.HIOS, RCI.CARRIER_NAME ORDER BY CW.HIOS",
  },
  {
    "Question": "How much inventory have we processed week over week per carrier via our manual process?",
    "SQL": "SELECT CARRIER_NAME, RESOLUTION_ACTION_SET_DT, COUNT(HIOS) AS InventoryCount FROM RAW.RECONInventory WHERE RESOLUTION_ACTION = 'Manual Review' GROUP BY CARRIER_NAME, RESOLUTION_ACTION_SET_DT",
  },
  {
    "Question": "What are the top ERR rejections per HIOS per Carrier month over month?",
    "SQL": "SELECT CARRIER_NAME, HIOS, DISPOSITION, COUNT(DISPOSITION) AS COUNT, FORMAT_DATE('%Y-%m', DATE(SUBMISSION_DATE)) AS DATE FROM `hps-aca-recon-dev-a210.DISPUTE.ERR` WHERE ERR_STATUS = 'Rejected' GROUP BY CARRIER_NAME, HIOS, DISPOSITION, FORMAT_DATE('%Y-%m', DATE(SUBMISSION_DATE)) ORDER BY DATE DESC, COUNT DESC",
  },
  {
    "Question": "What is the trend for discrepancy types for the last 2 years per carrier?",
    "SQL": "SELECT CARRIER_NAME, PLAN_YEAR, DISC_STATUS_NAME, FILE_TYPE_NAME, Count(DISC_STATUS_NAME) as DISCPCOUNT FROM RAW.RECONInventory GROUP BY CARRIER_NAME, PLAN_YEAR, DISC_STATUS_NAME, FILE_TYPE_NAME",
  },
  {
    "Question": "What percentage of subscribers/members do not have a financial discrepancy at present?",
    "SQL": "SELECT (SELECT COUNT(DISTINCT(EXCH_MEM_ID)) AS COUNT FROM RAW.RECONInventory WHERE PLAN_YEAR = 2024 and DISC_STATUS_NAME IN ('New','Carryover') and FILE_TYPE_NAME = 'RCNO' ) / (SELECT COUNT(DISTINCT(EXCHANGE_MEMBER_ID)) AS COUNT FROM `ACA.MEMBERS` WHERE EFFECTIVE_DATE <= '2024-12-31' AND (TERM_DATE IS NULL or TERM_DATE >= '2023-12-31' )) * 100 AS Percentage",
  },
  {
    "Question": "What percentage of subscribers/members do not have a financially impacting enrollment discrepancy at present?",
    "SQL": "SELECT (SELECT COUNT(DISTINCT(EXCH_MEM_ID)) AS COUNT FROM RAW.RECONInventory WHERE PLAN_YEAR = 2024 and DISC_STATUS_NAME IN ('New','Carryover') and FILE_TYPE_NAME = 'RCNO' ) / (SELECT COUNT(DISTINCT(EXCHANGE_MEMBER_ID)) AS COUNT FROM `ACA.MEMBERS` WHERE EFFECTIVE_DATE <= '2024-12-31' AND (TERM_DATE IS NULL or TERM_DATE >= '2023-12-31' )) * 100 AS Percentage",
  },
  {
    "Question": "What is the average rate of discrepancy resolution for any given HIOS ID/Carrier?",
    "SQL": "SELECT RESOLUTION_ACTION, RESOLUTION_STATUS, Avg(DATE_DIFF(RESOLUTION_ACTION_SET_DT, OPEN_DATE, DAY)) AvgRateDiscDays, Count(*) as Total FROM RAW.RECONInventory WHERE HIOS = 53882 Group By RESOLUTION_ACTION, RESOLUTION_STATUS, RESOLUTION_ACTION_SET_DT",
  },
  {
    "Question": "Which discrepancy types have seen a positive/negative trend of resolution over the last couple of cycles?",
    "SQL": "SELECT DISC_NAME, RESOLUTION_STATUS, CYCLE_DATE, Count(*) as TOTALCount FROM `hps-aca-recon-dev-a210.RAW.RECONInventory` WHERE CYCLE_DATE >= DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY) GROUP BY DISC_NAME, RESOLUTION_STATUS, CYCLE_DATE ORDER BY CYCLE_DATE desc",
  },
  {
    "Question": "What is the trending on worked but still unresolved discrepancies?",
    "SQL": "SELECT DISC_STATUS_NAME, RESOLUTION_STATUS_CATEGORY, RESOLUTION_ACTION, COUNT(*) AS Trending FROM RAW.RECONInventory WHERE RESOLUTION_STATUS_CATEGORY = 'PROCESSED' AND RESOLUTION_ACTION IN ('Dispute','Pending') GROUP BY DISC_STATUS_NAME, RESOLUTION_STATUS_CATEGORY, RESOLUTION_ACTION",
  },
  {
    "Question": "What is the resolution rate of worked discrepancies for all associates?",
    "SQL": "SELECT round(avg(case when DISC_STATUS_NAME='Resolved' then 100.0 else 0.0 end),2) as ResolvedRate, round(avg(case when DISC_STATUS_NAME != 'Resolved' then 100.0 else 0.0 end),2) as NonResolvedRate FROM `RAW.RECONInventory` WHERE RESOLUTION_STATUS = 'Worked' AND RESOLUTION_OWNER_NAME = 'WHPS Recon Ops'",
  },
  {
    "Question": "What is the average handling time for discrepancies for all associates?",
    "SQL": "SELECT DISC_STATUS_NAME, RESOLUTION_OWNER_NAME, AVG(DATE_DIFF(RESOLUTION_OWNER_SET_DT, OPEN_DATE, Day)) AS AVGHANDLINGTIME FROM `hps-aca-recon-dev-a210.RAW.RECONInventory` WHERE RESOLUTION_OWNER_SET_DT is not null GROUP BY DISC_STATUS_NAME, RESOLUTION_OWNER_NAME",
  },
  {
    "Question": "What is the average handling time for discrepancies by discrepancy type?",
    "SQL": "SELECT DISC_NAME, AVG(DATE_DIFF(RESOLUTION_OWNER_SET_DT, OPEN_DATE, Day)) AS AVGHANDLINGTIME FROM `hps-aca-recon-dev-a210.RAW.RECONInventory` WHERE RESOLUTION_OWNER_SET_DT is not null AND DISC_STATUS_NAME = 'Resolved' GROUP BY DISC_NAME",
  },
  {
    "Question": "What percentage of discrepancies are associated with HICS cases?",
    "SQL": "SELECT DISC_CATEGORY, COUNT(*) AS TotalCount, (SELECT COUNT(*) FROM RAW.RECONInventory WHERE HICS_CASE_ID IS NOT NULL) AS HICSCount, ( ( (SELECT COUNT(*) FROM RAW.RECONInventory WHERE HICS_CASE_ID IS NOT NULL )/COUNT(*) )) * 100 AS Percent FROM RAW.RECONInventory GROUP BY DISC_CATEGORY",
  },
  {
    "Question": "What percentage of discrepancies are being addressed through automation?",
    "SQL": "SELECT DISTINCT RESOLUTION_ACTION, DISC_STATUS_NAME, COUNT(*)/ (SELECT COUNT(*) * 100 AS AUTORESPER FROM `RAW.RECONInventory` ) AS AUTOResolved FROM `RAW.RECONInventory` WHERE RESOLUTION_ACTION IN ('System Update', 'No Action Required') GROUP BY RESOLUTION_ACTION, DISC_STATUS_NAME",
  },
  {
    "Question": "What percentage FIEDs are associated with fully subsidized/partially subsidized members?",
    "SQL": "SELECT ROUND((SELECT COUNT(*) FROM `RAW.RECONInventory` RI  join  `ACA.APTC` AP on CAST(RI.DTL_CIM AS STRING) = AP.DTL_CIM join `ACA.MEMBER_PREMIUM` MP on MP.DTL_CIM = CAST(RI.DTL_CIM AS STRING)  WHERE RI.DISC_STATUS_ID in (1,2) AND RI.PLAN_YEAR = 2024  AND AP.STATUS = 'C' AND MP.STATUS = 'C')/ (SELECT count(*) FROM `RAW.RECONInventory`  WHERE PLAN_YEAR = 2024) * 100,2) as FIEDPercentage",
  },
  {
    "Question": "What is the ERR acceptance % and rejection % for Py 2023",
    "SQL": "SELECT ROUND((SELECT COUNT(*) FROM `DISPUTE.ERR` WHERE COVERAGE_YEAR = 2023 AND ERR_STATUS = 'Accepted')/ (SELECT COUNT(*) FROM `DISPUTE.ERR` WHERE COVERAGE_YEAR = 2023 AND ERR_STATUS in ('Accepted','Rejected') ) * 100,2) AS  AcceptedPercent,ROUND((SELECT COUNT(*) FROM `DISPUTE.ERR` WHERE COVERAGE_YEAR = 2023 AND ERR_STATUS = 'Rejected')/ (SELECT COUNT(*) FROM `DISPUTE.ERR` WHERE COVERAGE_YEAR = 2023 AND ERR_STATUS in ('Accepted','Rejected') ) * 100 ,2) AS  RejectedPercent",
  },
  {
    "Question": "What is the inventory of Enrollment discrepancies from the latest Reconciliation Files for Cigna by Exchange and HIOS ID for 2023?",
    "SQL": "SELECT HIOS, COUNT(ENR_INVENTORY_ID) FROM `hps-aca-recon-dev-a210.RAW.RECONInventory` WHERE CARRIER_NAME = 'CIGNA' AND PLAN_YEAR = 2023 GROUP BY HIOS",
  },
  {
    "Question": "Please provide the current disposition of all ER&R Disputes submitted between 2024-05-01 and 2024-05-31 for Cigna by Plan Year and HIOS.",
    "SQL": "SELECT HIOS, COVERAGE_YEAR, DISPOSITION, COUNT(DISPOSITION) AS Total FROM `hps-aca-recon-dev-a210.DISPUTE.ERR` WHERE OPEN_DATE >= '2024-05-01' AND SUBMISSION_DATE <= '2024-05-31' AND CARRIER_NAME = 'CIGNA' GROUP BY HIOS, COVERAGE_YEAR, DISPOSITION",
  },
  {
    "Question": "What details were submitted to CMS for Subscriber ID 0003503569 in the last Recon Cycle for 2024?",
    "SQL": "Select * From `RAW.RCNIRaw` Where EXCH_MEMB_ID = '0003503569'",
  },
  {
    "Question": "How many ILB Cases, Detail Cases, CIMs, Records exist for Subscriber ID 0003503569 for 2024? Provide Details.",
    "SQL": "-- Detail Cases SELECT CMD.DTL_CIM AS CIM, CMD.DTL_CASE_NUM AS CASENum, CMD.EFFECTIVE_DATE, CMD.TERM_DATE, 'DTL_CASE' AS TYPE FROM `ACA.CASE_MASTER_DTL` CMD WHERE CMD.EXCHANGE_SUBSCRIBER_ID = '0003503569' AND EXTRACT(YEAR FROM DATE (CMD.EFFECTIVE_DATE)) <= 2024 AND (EXTRACT(YEAR FROM DATE (CMD.TERM_DATE)) >= 2024 OR CMD.ACTIVE_CODE = 'CM') UNION ALL -- ILB Cases SELECT CMI.ILB_CIM AS CIM, CMI.ILB_CASE_NUM AS CASENum, CMI.EFFECTIVE_DATE, CMI.TERM_DATE, 'ILB_CASE' AS TYPE FROM `ACA.CASE_MASTER_ILB` CMI WHERE CMI.EXCHANGE_SUBSCRIBER_ID = '0003503569' AND EXTRACT(YEAR FROM DATE (CMI.EFFECTIVE_DATE)) <= 2024 AND (EXTRACT(YEAR FROM DATE (CMI.TERM_DATE)) >= 2024 OR CMI.ACTIVE_CODE = 'G')",
  },
  {
    "Question": "Has a Dispute Enrollment ever been submitted for Subscriber ID 700000019551 from the CMS in 2023? If yes, what was the disposition?",
    "SQL": "SELECT * FROM `DISPUTE.ERR` WHERE FFM_EXCH_ASSGN_MEM_ID = 'EXCH SUBSID' and COVERAGE_YEAR = YEAR AND HIOS = HIOS",
  },
  {
    "Question": "What is the volume of discrepancies per subscriber?",
    "SQL": "SELECT EXCH_SUB_ID, COUNT(EXCH_SUB_ID) as VOLOFDISCPERSUBS FROM `RAW.RECONInventory` GROUP BY EXCH_SUB_ID",
  },
  {
    "Question": "What are the resolution rates from month to month?",
    "SQL": "SELECT TIMESTAMP_TRUNC(RESOLUTION_STATUS_SET_DT, month) AS YYYYMM, COUNT(DISC_STATUS_NAME) AS ResolvedRate FROM `RAW.RECONInventory` WHERE DISC_STATUS_NAME = 'Resolved' GROUP BY DISC_STATUS_NAME, YYYYMM",
  },
  {
    "Question": "What is the volume of aging flags per subscriber?",
    "SQL": "SELECT EXCH_SUB_ID, AVG(DATE_DIFF(CYCLE_DATE,OPEN_DATE,Day)) AS AVGDays, COUNT(OPEN_DATE) AS OpenDateCount FROM `hps-aca-recon-dev-a210.RAW.RECONInventory` GROUP BY OPEN_DATE, EXCH_SUB_ID ORDER BY OpenDateCount desc",
  },
  {
    "Question": "Can you give a breakdown of discrepancies by type?",
    "SQL": "SELECT DISC_TYPE_ID, COUNT(DISC_TYPE_ID) FROM `RAW.RECONInventory` GROUP BY DISC_TYPE_ID",
  },
  {
    "Question": "Does Subscriber ID 700000019551 have an Enrollment discrepancy, Financial discrepancy or both identified in the latest 2024 Reconciliation file from CMS? If so, what was identified as discrepant?",
    "SQL": "Select * From `RAW.RECONInventory` WHERE EXCH_SUB_ID = '700000019551' AND DISC_STATUS_ID in (1,2) and PLAN_YEAR = 2024",
  },
  {
    "Question": "Does Subscriber ID 5962545 from CMS have more than one span of coverage for 05/2023?",
    "SQL": "SELECT count(MARKETPLACE_PLCY_SGMT_ID) FROM `hps-aca-recon-dev-a210.RAW.PreAuditRaw` p join `hps-aca-recon-dev-a210.ACA.HIOS` h on h.HIOS = cast(p.HIOS as string) WHERE EXCHANGE_MEMBER_ID = 5962545 AND PLAN_YEAR = 2023 AND BENEFIT_START_DATE <= '2023-05-01' AND BENEFIT_END_DATE >= '2023-05-01'AND BENEFIT_START_DATE != BENEFIT_END_DATE AND h.state = 'GA'",
  }
]

BIGQUERY_DATASET_ID = [
    "hps-aca-recon-dev-a210.ACA",
    "hps-aca-recon-dev-a210.DISPUTE",
    "hps-aca-recon-dev-a210.HICS",
    "hps-aca-recon-dev-a210.RAW"
]
list_datasets_func = FunctionDeclaration(
    name="list_datasets",
    description="Get a list of datasets that will help answer the user's question",
    parameters={
        "type": "object",
        "properties": {},
    },
)

list_tables_func = FunctionDeclaration(
    name="list_tables",
    description="List tables in a dataset that will help answer the user's question",
    parameters={
        "type": "object",
        "properties": {
            "dataset_id": {
                "type": "string",
                "description": "Dataset ID to fetch tables from.",
            }
        },
        "required": [
            "dataset_id",
        ],
    },
)

get_table_func = FunctionDeclaration(
    name="get_table",
    description="Get information about a table, including the description, schema, and number of rows that will help answer the user's question. Always use the fully qualified dataset and table names.",
    parameters={
        "type": "object",
        "properties": {
            "table_id": {
                "type": "string",
                "description": "Fully qualified ID of the table to get information about",
            }
        },
        "required": [
            "table_id",
        ],
    },
)

sql_query_func = FunctionDeclaration(
    name="sql_query",
    description="Get information from data in BigQuery using SQL queries",
    parameters={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "SQL query on a single line that will help give quantitative answers to the user's question when run on a BigQuery dataset and table. In the SQL query, always use the fully qualified dataset and table names.",
            }
        },
        "required": [
            "query",
        ],
    },
)

sql_query_tool = Tool(
    function_declarations=[
        list_datasets_func,
        list_tables_func,
        get_table_func,
        sql_query_func,
    ],
)

model = GenerativeModel(
    "gemini-1.5-pro-001",
    generation_config={"temperature": 0},
    tools=[sql_query_tool],
)

st.set_page_config(
    page_title="SQL Talk with BigQuery",
    page_icon="vertex-ai.png",
    layout="wide",
)

col1, col2 = st.columns([8, 1])
with col1:
    st.title("SQL Talk with BigQuery")
with col2:
    st.image("vertex-ai.png")

st.subheader("Powered by Function Calling in Gemini")

st.markdown(
    "[Source Code](https://github.com/GoogleCloudPlatform/generative-ai/tree/main/gemini/function-calling/sql-talk-app/)   •   [Documentation](https://cloud.google.com/vertex-ai/docs/generative-ai/multimodal/function-calling)   •   [Codelab](https://codelabs.developers.google.com/codelabs/gemini-function-calling)   •   [Sample Notebook](https://github.com/GoogleCloudPlatform/generative-ai/blob/main/gemini/function-calling/intro_function_calling.ipynb)"
)

with st.expander("Sample prompts", expanded=True):
    st.write(
        """
        - What kind of information is in this database?
        - What percentage of orders are returned?
        - How is inventory distributed across our regional distribution centers?
        - Do customers typically place more than one order?
        - Which product categories have the highest profit margins?
    """
    )

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"].replace("$", "\$"))  # noqa: W605
        try:
            with st.expander("Function calls, parameters, and responses"):
                st.markdown(message["backend_details"])
        except KeyError:
            pass

if prompt := st.chat_input("Ask me about information in the database..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""
        chat = model.start_chat()
        client = bigquery.Client()

        prompt += """
            Please give a concise, high-level summary followed by detail in
            plain language about where the information in your response is
            coming from in the database. Only use information that you learn
            from BigQuery, do not make up information.
            """

        response = chat.send_message(prompt)
        response = response.candidates[0].content.parts[0]

        print(response)

        api_requests_and_responses = []
        backend_details = ""

        function_calling_in_process = True
        while function_calling_in_process:
            try:
                params = {}
                for key, value in response.function_call.args.items():
                    params[key] = value

                print(response.function_call.name)
                print(params)

                if response.function_call.name == "list_datasets":
                    api_response = client.list_datasets()
                    api_response = BIGQUERY_DATASET_ID
                    api_requests_and_responses.append(
                        [response.function_call.name, params, api_response]
                    )

                if response.function_call.name == "list_tables":
                    api_response = client.list_tables(params["dataset_id"])
                    api_response = str([table.table_id for table in api_response])
                    api_requests_and_responses.append(
                        [response.function_call.name, params, api_response]
                    )

                if response.function_call.name == "get_table":
                    api_response = client.get_table(params["table_id"])
                    api_response = api_response.to_api_repr()
                    api_requests_and_responses.append(
                        [
                            response.function_call.name,
                            params,
                            [
                                str(api_response.get("description", "")),
                                str(
                                    [
                                        column["name"]
                                        for column in api_response["schema"]["fields"]
                                    ]
                                ),
                            ],
                        ]
                    )
                    api_response = str(api_response)

                if response.function_call.name == "sql_query":
                    job_config = bigquery.QueryJobConfig(
                        maximum_bytes_billed=100000000
                    )  # Data limit per query job
                    try:
                        cleaned_query = (
                            params["query"]
                            .replace("\\n", " ")
                            .replace("\n", "")
                            .replace("\\", "")
                        )
                        query_job = client.query(cleaned_query, job_config=job_config)
                        api_response = query_job.result()
                        api_response = str([dict(row) for row in api_response])
                        api_response = api_response.replace("\\", "").replace("\n", "")
                        api_requests_and_responses.append(
                            [response.function_call.name, params, api_response]
                        )
                    except Exception as e:
                        api_response = f"{str(e)}"
                        api_requests_and_responses.append(
                            [response.function_call.name, params, api_response]
                        )

                print(api_response)

                response = chat.send_message(
                    Part.from_function_response(
                        name=response.function_call.name,
                        response={
                            "content": api_response,
                        },
                    ),
                )
                response = response.candidates[0].content.parts[0]

                backend_details += "- Function call:\n"
                backend_details += (
                    "   - Function name: ```"
                    + str(api_requests_and_responses[-1][0])
                    + "```"
                )
                backend_details += "\n\n"
                backend_details += (
                    "   - Function parameters: ```"
                    + str(api_requests_and_responses[-1][1])
                    + "```"
                )
                backend_details += "\n\n"
                backend_details += (
                    "   - API response: ```"
                    + str(api_requests_and_responses[-1][2])
                    + "```"
                )
                backend_details += "\n\n"
                with message_placeholder.container():
                    st.markdown(backend_details)

            except AttributeError:
                function_calling_in_process = False

        time.sleep(3)

        full_response = response.text
        with message_placeholder.container():
            st.markdown(full_response.replace("$", "\$"))  # noqa: W605
            with st.expander("Function calls, parameters, and responses:"):
                st.markdown(backend_details)

        st.session_state.messages.append(
            {
                "role": "assistant",
                "content": full_response,
                "backend_details": backend_details,
            }
        )
