import pandas as pd

class OutputHandler:
    def __init__(self, config):
        self.config = config

    def save_data_to_csv(self, data):
        output_csv_file = self.config.csv_file
        df = pd.DataFrame(data, columns=self.config.extractors.keys())
        df = self.process_dataframe(df)

        # Load existing data
        if os.path.exists(output_csv_file):
            df_existing = pd.read_csv(output_csv_file)
            df = pd.concat([df_existing, df], ignore_index=True)

        # Drop duplicates and save
        df.drop_duplicates(inplace=True)
        df.to_csv(output_csv_file, index=False)

    # def save_data_to_excel(self, data):
    #     output_excel_file = self.config.excel_file
    #     df = pd.DataFrame(data)

    #     # Write to Excel with xlsxwriter, creating the file if it doesn't exist
    #     with pd.ExcelWriter(output_excel_file, engine='xlsxwriter') as writer:
    #         df.to_excel(writer, index=False, sheet_name='iproperty')