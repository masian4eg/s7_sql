from convert_csv_json import convert_func
import time


def main():
    while True:
        convert_func()
        # делаем паузу 2 минуты до следующей проверки папки на новые файлы
        time.sleep(5)


if __name__ == '__main__':
    main()
