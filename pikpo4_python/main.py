from processor.dataprocessor_service import DataProcessorService

"""
    Main-модуль, т.е. модуль запуска приложений ("точка входа" приложения)
"""


if __name__ == '__main__':
    service = DataProcessorService(datasource="wood_removal_cubic_meters.csv", db_connection_url="sqlite:///test.db")
    service.run_service()
