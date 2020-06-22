#!/usr/bin/python3
#coding=UTF-8
'''
	The main module for launching the functions of performing 
	a posteriori daily monitoring based 
	on measurement files in the Rinex 3 format
'''
__author__ 		= "KomissarovAV"
__copyright__	= "Copyright 2019, AO GLONASS"
__license__		= "Secret Developments"
__email__		= "av.komissarov@glonass-iac.ru"
__version__		= "0.5.0"
__data__		= "2020-03-18"

print('Modules connecting...')# основные требуемые библиотеки
import time
import sys
import os
import io
print('Modules are connected')

# Основная программа
time_start = time.time()

print('Path connecting...')
from dir_function import create_dir_path
# загрузка путей
dir_path = create_dir_path()
print('Path connecting...')

'''
				Подключение библиотек			
'''
print('Lib connecting...')
sys.path.insert(0,dir_path['path_lib'])			# путь до каталога с функциями
from datetime_function import *					# библиотека времени
from python_dbconfig import connect_to_DB		# подключение к БД
from alarm import *								# библиотеки рабыты по остановке работы программы
from logging_file import *							# формирование лог-данных и лог-файла

'''		Этап 1 - формирование начальных данных		'''
from preparation_file import *					# функции копирование/перемещение файлов
from preparation_alm import *					# функции копирование/перемещение альманахов
from processing_file import *					# функции изменение файлов
from path_function import * 					# функции изменение и работы с директориями
from zona import zona							# функция создания зон через  CALC_ZON.exe
'''		Этап 2 - занесение данных в БД		'''
from entry_data_to_DB import *					# занесение данных мониторинга в БД
from alma import *								# занесение данных альманаха в БД

'''		Этап 3 - обработка данных в БД		'''
from heft_new import *								# создание и другие действия с весами станций
from mon_analysis import * 						# формирование выборки по мониторингу
from nav_analysis import * 						# формирование выборки по навигационным сообщениям
from sample_analysis import *					# формирование объединеной выборки и выборки для отчета
from genchar import genchar						# формирование таблицы genchar
'''		Этап 4 - формирование отчета		'''
from image import *								# создание картинок
#from russer import *                           # формирование текстового файла данных для ТИ и РС
print('Lib are connected')

print('wine clean start...')					# основные очистки
wine_restart()					
print('wine clean end')
'''
				Начало программы			
'''
if __name__ == "__main__":
	#for days_d in [4,3,2,2,1]:
		days_d = 1
		try:
			'''
				ЧАСТЬ 0 ПОДГОТОВКА
			'''
			# вычисление занчений даты
			year, month, day, day_year, hour, now_date = create_datetime_item(days_d)
			print(year,month,day,day_year)
			
			#дополнение путей изменяемыми данными
			dir_path = redefinition_dir_path(dir_path, now_date)

			#### создание файла-лога
			name_log_file = '/logs_{0}.txt'.format(now_date.strftime('%d.%m.%Y'))
			f = create_log_file(dir_path,now_date,name_log_file)
			file_log_header(f, now_date, day_year, hour, 1)
			## подключение к БД
			try:
				cnx = connect_to_DB(f, 'ConnectDailyMonDB.cfg')
				flagConnect = False
			except:
				flagConnect = True
			finally:
				pass

			'''
				Часть 1 ПРОВЕРКА ПУТЕЙ
			'''			
			## пути глобальные
			print('--- checkglobpath')
			flag_path = True
			flag_path = check_glob_path(f,dir_path)
			
			## пути локальные
			print('--- checklocpath')
			check_loc_path(f,dir_path)

			'''
				ЧАСТЬ 2	СОЗДАНИЕ И ЗАНЕСЕНИЕ ВЫБОРКИ
			'''
			print("--- osremove")
			## удаление файлов в рабочей папке
			osremove(dir_path['path_izm_bef_file'],f,0)
			osremove(dir_path['path_bds'],f,1)
			f.flush()
				
			print("--- copy_measurement_files")
			#### копирование данных на localmachin
			copy_measurement_files(cnx,f,dir_path, day_year, year)
			f.flush()
			
			print("--- del_rn2")
			#### удаления повторяющихся файлов во 2ом rinex
			del_file_rn2(dir_path['path_izm_bef_file'],f)
			f.flush()
				
			print('--- copy_nav_file')
			#### копирование навигационных данных
			''' flag_efm возвращение 
					0 - данных нет	от внутренней склейки
					1 - данные есть от внутренней склейки'''
			flag_efm = copy_nav_file(dir_path,day_year,year,f)
			f.flush()
			if (flag_path == False and flag_efm != 1):
				f.write(u'ОШИБКА:	Навигационных данных нет - Завершение работы\n')
				exit(1)
				f.flush()
	
			print('--- unpacking')
			#### разархивирование архивов измерений
			unpacking_file(f,dir_path['path_izm_bef_file'],dir_path['path_add_pro'])
			f.flush()

			print('--- mov_almanach')
			#### копирование альманхов
			copy_almanach_file(f,day,month,year,dir_path)

			if (int(hour) < 10):
				osremove(dir_path['path_zona'],f,0)

				print('--- zona')
				#### создание зон
				zona(cnx,dir_path,year,month,day,day_year,f)
				f.flush()
				
			'''
				ЧАСТЬ 3	ФОРМИРОВАНИЕ ДАННЫХ МОНИТОРИНГА
			'''
			print('--- sat_sol_create')
			#### распаковка файла в файлы с расширением SAT и SOL программой BDS_BDS.exe
			f.write(u'----------sat_sol_create\n')
			sat_sol_create(dir_path['path_izm_bef_file'],dir_path['path_bds'],f)
			
			####удаление данных с измерениями
			osremove(dir_path['path_izm_bef_file'],f,0)
			f.flush()          
			
			print('--- copy_sat_sol_file')
			#### перемещение файлов sat и sol
			f.write(u'----------copy_sat_sol_file\n')            
			copy_sat_sol_file(f,dir_path['path_bds'],dir_path['path_sat_sol'],day_year)
			f.flush()  
				
			'''
				ЧАСТЬ 4 ЗАНЕСЕНИЕ ДАННЫХ МОНИТОРИНГА В БД
			'''
			if flagConnect:
				f.write(u'\nЗавершение работы, так как нет подключения к БД\n'
						u'\n--------------------------------------------------\n'
						u'Программный комплекс работал: {:.3f} мин.\n'.format((time_exit - time_start)/60))
				f.flush()
				exit(1)

			print('--- mass_data_enter_into_DB')
			#### занесение данных в БД
			mass_data_enter_into_DB(cnx,f,day,month,year,dir_path['path_sat_sol'],dir_path['path_sql_input_data'])
			f.flush()
			
			print('--- almanach')
			#### занесение данных-альманаха в БД
			almanach(cnx,f,day,month,year,day_year,dir_path)
			#almanach(cnx,f,day,month,year,day_year,path_alm,path_alm_loc,path_alm_gal_loc,path_alm_bds_loc,path_add_pro,path_alm_txt_loc)
			f.flush()
			
			'''
				ЧАСТЬ 5.1 СОЗДАНИЕ ВЕСОВ
			'''

			# print('--- create_heft')
			# #### функция создания весов (Есипов П.А.)
			# create_heft(cnx,f,year,month,day)
			# f.flush()
			# print('--- check_heft')
			# #### функция дополнения весов которых нет
			# check_heft(cnx,f,year,month,day)
			# f.flush()

			'''
				ЧАСТЬ 5.2 АНАЛИЗ ДАННЫХ МОНИТОРИНГА
			'''
			
			print('--- int_acc_insert')
			#### процесс формирование выборки (занесение данных в таблицу int_acc_sat)
			int_acc_sat_insert(year,month,day,cnx,f)
			f.flush()
			
			print('--- mon_int_acc')
			#### формирование признаков на эпоху
			mon_int_acc(cnx,f,year,month,day,dir_path['path_sql_input_data'])
				   
			print('--- mon_int_acc_upd')
			#### изменение признаков на эпоху
			mon_int_acc_upd(cnx,f,year,month,day)
			f.flush()
			
			print('--- mon_spans')
			#### формирование промежутков точности
			create_spans(cnx,'mon_spans',f,year,month,day)
			
			if flag_efm == 0:
				print('--- mon_spans_met')
				##### промежутков состояния точности на основе нав.файла от (МИТРИКАС)
				mon_spans_met(cnx,f,year,month,day)
				
			
			'''
				ЧАСТЬ 5.3 ОБЪЕДИНЕНИЕ ДАННЫХ И ФОРМИРОВАНИЕ ИТОГОВОЙ ИНФОРМАЦИИ
			'''
			print('--- mon_nav_int_acc')
			#### формирование признаков на эпоху
			mon_nav_int_acc(cnx,f,year,month,day)
			
			print('--- mon_nav_spans')
			#### формирование промежутков точности
			create_spans(cnx,'mon_nav_spans',f,year,month,day)
			
			print('--- daily_mon')
			###занесение почасового состояния спутников в БД для Бюллютеня
			daily_mon(cnx,f,year,month,day)
			f.flush()  

			###print('--- blt_spans') -- не реализовано blt_int_acc
			##### формирование промежутков точности
			###сreate_spans(cnx,'blt_spans',f,year,month,day)
			
			'''
				ЧАСТЬ 6	ФОРМИРОВАНИЕ ХАРАКТЕРИСТИК ФУНКЦИОНИРОВАНИЯ СИСТЕМЫ
			'''
			print('--- genchar') 
			# занесение Обобщённые характеристики функционирования систем за сутки для бюллютеня
			genchar(cnx,f,year,month,day)
			
			###print('russer') 
			##### формирование текстового файла данных для ТИ и РС
			####russer(cursor,f,cnx,year,month,day,day_year,path_root,path_pdf+'/doctext')
			###print('russer_end')
						
			'''
				ЧАСТЬ 7	ФОРМИРОВАНИЕ БЮЛЛЕТЕНЯ
			'''
			print('--- image_main')
			#### создание графиков
			image_main(year,month,day,cnx,f,dir_path['path_image'])
			f.flush()
			
			print('--- otch_create')
			#### вызов программы-порадителя pdf
			print("Начали создание pdf")
			timpdfop = time.time()
			cnx.commit()
			print(dir_path['path_NewOtchPDF'] + '/NewOtchPDF {0}'.format(days_d))
			f.write(u'Идет процесс формирования pdf ...\n')
			cmd = "{0}/NewOtchPDF {1}".format(dir_path['path_NewOtchPDF'],days_d)
			f.write(u'{0}\n'.format(cmd))
			print('ST_____')
			os.system(cmd)  
			print('END_____')          
			timpdfex = time.time() 
			
			print('pdf_copy_to_loc')
			#### перемещение файла-лога pdf-файла
			pdf_copy_to_loc(year,month,day, dir_path['path_main'], dir_path['path_logs_NewOtchPDF'])
			f.write(u'Создание pdf-файла заняло {0} сек.\n'.format(timpdfex - timpdfop))
			f.flush()
			
			print('pdf_copy')
			### перемещение файла pdf
			f.write('----------pdf_copy\n')
			print(dir_path['path_pdf'])
			pdf_copy(f, year,month,day, dir_path['path_pdf_loc'], dir_path['path_pdf'], 0)
			f.flush()

			cnx.commit()
			cnx.close()
			
		except Exception as err:
			f.write("Неожиданная ошибка: {0}\n".format(err))
			f.flush()
			print("Неожиданная ошибка: {0}\n".format(err))
			f.write(u'Скрипт перестал работать --------------------------------------------------\n')
			f.flush()
			raise
			
			## =====================================================================
			## ===================== КОНЕЦ =========================================
			## =====================================================================
		finally:
			time_exit = time.time()
			f.write(u'\n--------------------------------------------------\n'
					u'Программный комплекс работал: {:.3f} мин.\n'.format((time_exit - time_start)/60))
			now_date = datetime.datetime.now()
			f.write(u'Окончание работы: {0}\n==========================================================================\n'.format(now_date.strftime('%H:%M:%S')))
			f.close()
