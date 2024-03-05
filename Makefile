install:
		@pip install -e .

reload_env:
		@direnv reload

clean_gen_register:
		rm ${DATA_CSV_ENERGY_PRODUCTION_PATH}/energy_production_register.csv

clean_gen_data:
		make clean_gen_register
		rm ${DATA_CSV_ENERGY_PRODUCTION_PATH}/*.csv

clean_meteo_data:
		rm ${DATA_CSV_METEO_PATH}/*.csv

clean_all_data:
		make clean_gen_data
		make clean_meteo_data
