# %%
#Scipt to create tables for visualization 

# %%
!pip install mysqlclient
!pip install sqlalchemy
!pip install python-dotenv psycopg2-binary
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import psycopg2

# %%
# Load environment variables
load_dotenv()

# %%
#Set up Postgres connection

pg_user = os.environ['PG_USER']
pg_password = os.environ['PG_PASSWORD']
pg_host = os.environ['PG_HOST']
pg_db = os.environ['PG_DB']
pg_conn_str = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_db}'
pg_engine = create_engine(pg_conn_str)

# %%
#Question 1: What does the compensation among the 3 best paid executives look like among nonprofit categories?

# %%
query = '''


with org_comp as (
	select *
	from staging.staging_org_comp soc
	where has_employee = TRUE
), nonprofit_comp as (
select
	oc.ein as ein,
	sn.name as name,
	sn.ntee_category as category,
	first_employee_compensation as first_c,
	second_employee_compensation as second_c,
	third_employee_compensation as third_c,
	first_employee_compensation + second_employee_compensation + third_employee_compensation as total_comp,
	AVG(first_employee_compensation + second_employee_compensation + third_employee_compensation) over (partition by oc.ein) as average_top_comp
from
	org_comp oc
left join
	staging.staging_nonprofits sn
on
	oc.ein = sn.ein
order by
	first_c desc
)
select
	category,
	MAX(first_c) as most_salary,
	ROW_NUMBER() over (order by MAX(first_c) DESC) as highest_salary_rank,
	ROUND(AVG(first_c)::numeric, 2) as avg_high_salary,
	ROW_NUMBER() over (order by ROUND(AVG(first_c)::numeric, 2) DESC) as average_highest_salary_rank,
	ROUND(AVG(average_top_comp)::numeric, 2) as avg_top_compensation,
	ROW_NUMBER() over (order by ROUND(AVG(average_top_comp)::numeric, 2) DESC) as avg_top_compensation_rank
from
	nonprofit_comp nc
group by
	category
order by
	ROUND(AVG(first_c)::numeric, 2) DESC	



'''
# %%
# Set pandas display options to show all columns and rows
pd.set_option('display.max_rows', None)

# %%
# Read the results from the query and display them
np_df = pd.read_sql(query, con=pg_engine)
# %%
# Show results
np_df
# %%
# Insight 
'''Executive compensation across nonprofit categories reveals both expected trends and curious disparities. Unsurprisingly, Health, Science & Technology, and Employment top the list for average top salaries and total leadership compensation — likely reflecting the technical expertise and market competitiveness needed in those fields.
Yet from a journalistic perspective, the Education sector stands out. It ranks #1 in total revenue yet only #4 in average salary, suggesting either a broader distribution of funds across programming or a potential undervaluation of executive leadership relative to sector size.
Meanwhile, mission-critical categories like Civil Rights, Mental Health, and Medical Research fall into a middle tier — not undercompensated, but clearly below high-revenue sectors. Conversely, sectors like Religion-Related and Recreation & Sports rank at the bottom, with average top salaries below $25,000, possibly due to smaller budgets, volunteer-led structures, or culturally lower expectations for executive pay.
These rankings hint at deeper dynamics around mission, funding access, and leadership priorities across the nonprofit ecosystem in Los Angeles.
'''
# %%
# Recommendation
'''
To ensure both fairness and impact, compensation structures should be benchmarked not just by revenue, but by mission intensity, program reach, and leadership demands. Boards and donors should evaluate whether executive pay reflects true organizational complexity and output — or whether some categories are underinvesting in leadership due to tradition or bias.
At the same time, watchdogs, analysts, or investigative reporters should delve deeper into outliers:
Why do some lower-revenue sectors rank higher in executive pay than larger sectors?
Are there signs of inefficiencies, or alternatively, are some sectors undervaluing leadership?
A deeper correlation analysis between executive pay, program delivery, and outcomes could yield more actionable benchmarks for sector-specific compensation norms.
'''

# %%
# Prediction
'''
As disparities become more visible, nonprofits and funders will push for compensation frameworks based on mission complexity, not just revenue or traditional norms.
Donors and watchdog groups will increasingly flag sectors where leadership compensation seems disproportionate to revenue or program output, triggering closer oversight or conditional funding.
Unless addressed directly, sectors like Religion-Related and Recreation & Sports will likely continue to attract fewer executive resources — potentially affecting their long-term sustainability or ability to scale. 
'''
# %%
#Question 2: What does the compensation look among the top 3 best paid executives among Los Angeles nonprofits?

# %%
query = '''


with top_exec_compensation as (
	SELECT
       soc.ein,
       sn.name,
       sn.ntee_category,
       sn.zipcode,
       soc.first_employee_compensation AS first_c,
       soc.second_employee_compensation AS second_c,
       soc.third_employee_compensation AS third_c,
       (soc.first_employee_compensation + soc.second_employee_compensation + soc.third_employee_compensation) AS total_top_3_comp
   FROM staging.staging_org_comp soc
   LEFT JOIN staging.staging_nonprofits sn ON soc.ein = sn.ein
   WHERE soc.has_employee = TRUE
), comp_with_expense AS (
  SELECT
       tec.ein,
       tec.name,
       tec.ntee_category,
       tec.zipcode,
       tec.total_top_3_comp,
       snf.expenses
   FROM top_exec_compensation tec
   LEFT JOIN staging.staging_nonprofit_financials snf ON tec.ein = snf.ein
   WHERE snf.expenses IS NOT NULL AND snf.expenses > 0
)
SELECT
   ein,
   name,
   ntee_category,
   zipcode,
   total_top_3_comp,
   expenses,
   ROUND(((total_top_3_comp / expenses)*100)::numeric, 2) AS top_3_comp_pct_of_expenses
FROM comp_with_expense
ORDER BY top_3_comp_pct_of_expenses DESC

  

'''
# %%
# Set pandas display options to show all columns and rows
pd.set_option('display.max_rows', None)

# %%
# Read the results from the query and display them
np_df = pd.read_sql(query, con=pg_engine)
# %%
# Show results
np_df
# %%
'''Insights
While average compensation levels across nonprofit sectors appear reasonable, a closer diagnostic analysis reveals that some organizations allocate over 100% of their total expenses to executive compensation. 
In dozens of cases, 70–100% of total spending went solely to the top three executives. This suggests either financial mismanagement, lack of operational programs, or a strategic misuse of nonprofit status. 
For example, organizations like American Industrial Real Estate Association and Good Samaritan Institute had comp ratios of 119.96% and 101.96%, respectively — implying either severe deficits or intentional resource funneling to leadership.
'''
# %%
# Recommendation
'''
Analyze whether organizations with >100% compensation ratios are running annual deficits, delaying vendor payments, or relying on debt or reserves to fund leadership.
Cross-reference comp-heavy organizations with 990 fields like “program service accomplishments” or “grants paid” to see if money is being funneled to services or staying at the top.
Identify whether these outliers are concentrated in certain sectors (e.g., real estate, associations), specific zip codes, or income brackets — this may reveal patterns of exploitation or structural risks.
Determine if the high ratios are one-time anomalies or recurring. A sustained pattern may indicate chronic misuse of funds or a dormant shell organization.
Evaluate whether these outliers are on charities beyond 501(c) 
'''
# %%
# Prediction
'''
Expect demands for clearer justification of compensation relative to program reach, especially among smaller orgs or those claiming public benefit.
Just as Charity Navigator evaluates financial health, a new wave of rating systems may score nonprofits on “mission ROI” — aligning compensation with social return.
'''