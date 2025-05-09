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
#Question 1: What does the nonprofit sector look like across zipcodes? 

# %%
#Create query focused on nonprofit sector
query = '''
with stg_fin_his_metrics as (
select
	DATE(date_year) as year,
	SUM(totrevenue) as historical_revenue,
	MAX(totrevenue) as max_revenue,
	SUM(totfuncexpns) as historical_expense,
	MAX(totfuncexpns) as max_expense,
	SUM(totassetsend) as historical_assets,
	SUM(totliabend) as historical_liabilities
from
	staging.staging_financial_history
group by
	date_year
having
	date_year between '2007-01-02' and '2023-01-01'
order by
	date_year DESC
) select
	year,
	historical_revenue,
	max_revenue,
	historical_expense,
	max_expense,
	historical_assets,
	historical_liabilities,
	ROUND(((historical_revenue - historical_expense) / historical_revenue) * 100, 2) as historical_op_margin_pct,
	ROUND((historical_revenue / historical_assets) * 100, 2) as historical_asset_turnover_prc,
	ROUND((historical_liabilities / historical_assets) * 100, 2) as historical_debt_asset_prc,
	ROUND((historical_liabilities / historical_revenue) * 100, 2) as historical_liabilities_to_revenue_prc
from stg_fin_his_metrics
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
# Insight from the dataframe
'''
The nonprofit sector in Los Angeles has seen dramatic financial growth since the 2010s, with total revenue jumping from $13 million in 2010 to over $83 million in 2023 (note: 2023’s data is limited). However, this growth hasn’t always translated into stability: 2016 and 2010 saw net operating losses, and debt-to-asset ratios peaked dangerously high in years like 2013 (90%) and 2017 (66%).
A striking trend is the volatility of liabilities compared to revenue. In 2013, liabilities represented more than 1,200% of revenue, suggesting either massive underreporting of revenue or unsustainable borrowing patterns.
'''

# %%
#Recommendations 
'''
From a business perspective, nonprofits should set internal benchmarks for financial health — such as keeping debt-to-asset ratios below 50% and maintaining positive operating margins annually.
From a journalistic lens, this trend raises questions about financial transparency and oversight. Funders and watchdogs should investigate why liabilities surged disproportionately in certain years and whether those debts were used to expand programs — or cover deficits.
'''
# %%
#Prediction
'''
As philanthropic and government funding patterns continue to shift post-pandemic, expect to see:

Consolidation of smaller nonprofits

Greater focus on financial efficiency metrics

Journalistic scrutiny on organizations with high executive pay but low program delivery
'''
# %%

#What about the nonprofit sector across zipcodes? 

# %%
#Create query focused on nonprofit sector

query = '''
WITH nonprofit_financial_history AS (
   SELECT
       sfh.ein AS ein,
       sn.name AS name,
       sn.zipcode AS zipcode,
       sfh.year AS year,
       sfh.totrevenue AS revenue,
       sfh.totfuncexpns AS expenses,
       sfh.totassetsend AS assets,
       sfh.totliabend AS liabilities
   FROM staging.staging_financial_history sfh
   LEFT JOIN staging.staging_nonprofits sn ON sfh.ein = sn.ein
),
zip_financials AS (
   SELECT
       zipcode,
       year,
       SUM(revenue) AS zip_revenue,
       SUM(expenses) AS zip_expenses,
       SUM(assets) AS zip_assets,
       SUM(liabilities) AS zip_liabilities
   FROM nonprofit_financial_history
   GROUP BY zipcode, year
),
sector_totals AS (
   SELECT
       year,
       SUM(revenue) AS total_sector_revenue
   FROM nonprofit_financial_history
   GROUP BY year
),
zip_with_sector AS (
   SELECT
       zf.*,
       st.total_sector_revenue,
       ROUND(100.0 * zf.zip_revenue / NULLIF(st.total_sector_revenue, 0), 2) AS revenue_share_pct
   FROM zip_financials zf
   LEFT JOIN sector_totals st ON zf.year = st.year
),
zip_with_growth AS (
   SELECT
       *,
       ROUND(100 * (zip_revenue - LAG(zip_revenue) OVER (PARTITION BY zipcode ORDER BY year))
             / NULLIF(LAG(zip_revenue) OVER (PARTITION BY zipcode ORDER BY year), 0), 2) AS revenue_growth_pct,
       ROUND(100 * (zip_expenses - LAG(zip_expenses) OVER (PARTITION BY zipcode ORDER BY year))
             / NULLIF(LAG(zip_expenses) OVER (PARTITION BY zipcode ORDER BY year), 0), 2) AS expense_growth_pct
   FROM zip_with_sector
)
SELECT
   zipcode,
   year,
   zip_revenue,
   zip_expenses,
   zip_assets,
   zip_liabilities,
   revenue_growth_pct,
   expense_growth_pct,
   ROUND((zip_revenue - zip_expenses) / NULLIF(zip_revenue, 0) * 100, 2) AS op_margin_pct,
   ROUND(zip_liabilities / NULLIF(zip_assets, 0) * 100, 2) AS debt_asset_ratio_pct,
   revenue_share_pct
FROM zip_with_growth
where year between 2013 and 2023
ORDER BY zipcode, year;
'''

# %%
# Set pandas display options to show all columns and rows
pd.set_option('display.max_rows', None)

# %%
# Read the results from the query and display them
np_df = pd.read_sql(query, con=pg_engine)
# %%
# Show results
np_df.head(10)
# %%

# Insight from the dataframe
'''
Without looking at each individual zipcodes, there are significantly distinct outliers within growth in the nonprofit sector
90089 (USC campus) and 90048 (Cedar Sinai Medical Hospital) skew the data heavily (created drop down to eliminate zipcodes for better analysis)
Zipcodes periodically increase in all financial metrics (revenue, expenses, liabilities, assets)
Largest difference remains in assets --> possible way of nonprofits gaining leverage over their loans or accumulating wealth without too much revenue

'''

# %%
#Recommendations 
'''
Encourage nonprofit watchdogs or researchers to explore how nonprofits are building assets faster than revenue.
Are these asset increases tied to large donations, real estate holdings, or retained earnings?
Examine whether high asset levels correlate with strategic loan use (e.g., are assets being used as collateral or to avoid program spending?).
'''
# %%
#Prediction
'''
Expect philanthropic and civic analysts to prioritize zip-level adjusted insights that remove major institutions from community-level funding analysis.
Asset building strategies will continue POST-Covid for longterm stabilitiy
Without much research on funding techinques on my end, it would be advised funding decisions should be moved toward equity-based resource distribution over absolute metrics
'''
# %%