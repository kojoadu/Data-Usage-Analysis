import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import aiohttp
import asyncio
from modules.util import get_max_date, get_min_date

st.title("FTTX Data Usage Analysis")

# Function to get ISP from ip-api.com asynchronously
async def get_isp(session, ip):
    try:
        async with session.get(f"https://api.findip.net/{ip}/?token=5d0d092fb47046dbb8f3f7be6617c058") as response:
            if response.status == 200:
                data = await response.json()
                if 'traits' in data:
                    traits = data['traits']
                    isp = traits.get('isp', 'Unknown')
                    return isp
                else:
                    return 'Unknown'
            else:
                return f"Error: Status code {response.status}"
    except Exception as e:
        return f'Error: {e}'

# Function to update Application_Type column asynchronously
async def update_application_type(df):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for index, row in df[df['Application_Type'] == 'Other_UDP'].iterrows():
            ip = row['Server_IP']
            tasks.append(asyncio.ensure_future(get_isp(session, ip)))

        isps = await asyncio.gather(*tasks)
        
        # Update the DataFrame with the obtained ISP values
        df.loc[df['Application_Type'] == 'Other_UDP', 'Application_Type'] = isps
    return df

@st.cache_data
def process_data(df):
    with st.spinner('Processing data...'):
        try:
            # Renaming columns for consistency
            df = df.rename(columns={
                'Category Type': 'Category_Type',
                'Application Type': 'Application_Type',
                'Start Time': 'Start_Time',
                'End Time': 'End_Time',
                'Total Traffic(B)': 'Total_Traffic_B',
                'Server IP': 'Server_IP'
            })

            # Ensure the required columns exist
            required_columns = ['Total_Traffic_B', 'Start_Time', 'End_Time', 'Server_IP', 'Application_Type']
            if all(col in df.columns for col in required_columns):
                df['Total_Traffic_GB'] = df['Total_Traffic_B'] / (1024 * 1024 * 1024)
                df['Start_Time'] = pd.to_datetime(df['Start_Time'])
                df['End_Time'] = pd.to_datetime(df['End_Time'])

                # Update Application_Type for Other_UDP
                df = asyncio.run(update_application_type(df))
            else:
                st.error('The uploaded file does not contain the required columns.')
                return pd.DataFrame()  # Return an empty dataframe on error

        except Exception as e:
            st.error(f"Error processing data: {e}")
            return pd.DataFrame()  # Return an empty dataframe on error

    return df

with st.form('read_data'):
    fl = st.file_uploader(":file_folder: Upload your file", type=["csv", "txt", "xlsx", "xls"])
    submit_button = st.form_submit_button('Submit')
    
    if fl is not None:
        with st.spinner('Reading file...'):
            try:
                if fl.name.endswith('.csv') or fl.name.endswith('.txt'):
                    df = pd.read_csv(fl, encoding="ISO-8859-1")
                elif fl.name.endswith('.xlsx') or fl.name.endswith('.xls'):
                    df = pd.read_excel(fl)

                fbb_df = process_data(df)
                if not fbb_df.empty:
                    st.session_state['fbb_df'] = fbb_df
                    st.success("File uploaded and processed successfully!")
                else:
                    st.error("Failed to process file. Please check the file format and data.")
            
            except Exception as e:
                st.error(f"Error reading file: {e}")

if 'fbb_df' in st.session_state:
    fbb_df = st.session_state['fbb_df']
    
    # Second form: Analyze data
    with st.form('analyze_data'):
        categories_list = fbb_df['Category_Type'].unique()
        select_all = st.checkbox('Select All Categories')
        
        if select_all:
            selected_categories = st.multiselect('Select Categories', categories_list, default=categories_list)
        else:
            selected_categories = st.multiselect('Select Categories', categories_list)
        
        col1, col2, col3 = st.columns([1, 1, 1])

        min_date_ff = get_min_date(fbb_df)
        max_date_ff = get_max_date(fbb_df)

        start_date = col1.date_input('From', value=min_date_ff, max_value=max_date_ff, min_value=min_date_ff)
        end_date = col2.date_input('To', value=max_date_ff, max_value=max_date_ff, min_value=min_date_ff)

        # Filter by application type
        selected_application = col3.selectbox('Select Application Type', ['All'] + list(fbb_df['Application_Type'].unique()))
        
        submit_button = st.form_submit_button('Analyze')

        if submit_button:
            if start_date > end_date:
                st.error('Start date must be before or equal to the end date.')
            else:
                with st.spinner('Filtering data...'):
                    filtered_df = fbb_df[
                        (fbb_df['Category_Type'].isin(selected_categories)) &
                        (fbb_df['Start_Time'] >= pd.to_datetime(start_date)) &
                        (fbb_df['Start_Time'] <= pd.to_datetime(end_date))
                    ]
                    
                    if selected_application != 'All':
                        filtered_df = filtered_df[filtered_df['Application_Type'] == selected_application]

                if not filtered_df.empty:
                    # Category chart using plotly express
                    with st.spinner('Generating category chart...'):
                        category_traffic = filtered_df.groupby('Category_Type').agg({'Total_Traffic_GB': 'sum'}).reset_index()
                        category_traffic = category_traffic.sort_values(by='Total_Traffic_GB', ascending=False)

                        fig = px.bar(category_traffic, x='Total_Traffic_GB', y='Category_Type', orientation='h', 
                                     title='Usage per Category', labels={'Total_Traffic_GB': 'Total Traffic (GB)'}, template="seaborn",
                                     color='Category_Type', color_discrete_sequence=px.colors.qualitative.Set3)
                        fig.update_layout(yaxis_title='Category Type')
                        st.plotly_chart(fig)

                    # Displaying application traffic across filtered data
                    with st.spinner('Generating overall application traffic chart...'):
                        application_traffic = filtered_df.groupby('Application_Type').agg({'Total_Traffic_GB': 'sum'}).reset_index()
                        application_traffic = application_traffic.sort_values(by='Total_Traffic_GB', ascending=False)

                        fig5 = px.bar(application_traffic, x='Application_Type', y='Total_Traffic_GB', title='Overall Application Traffic', 
                                      color='Application_Type', labels={'Total_Traffic_GB': 'Total Traffic (GB)'}, 
                                      color_discrete_sequence=px.colors.qualitative.Set3)
                        st.plotly_chart(fig5)

                    # Time series for selected application
                    if selected_application != 'All':
                        with st.spinner(f'Generating time series for {selected_application}...'):
                            app_df = filtered_df[filtered_df['Application_Type'] == selected_application]
                            fig_time_series = px.line(app_df, x='Start_Time', y='Total_Traffic_GB', 
                                                      title=f'Time Series for {selected_application}',
                                                      labels={'Total_Traffic_GB': 'Total Traffic (GB)', 'Start_Time': 'Time'})
                            st.plotly_chart(fig_time_series)

                else:
                    st.warning("No data available for the selected criteria.")
