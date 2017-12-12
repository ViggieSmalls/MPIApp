import argparse

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--files', nargs='+')
args = parser.parse_args()
print(args.files)



# elif args.test:
# logger.info('Starting MPIApp in test mode')
# import shutil
# from time import sleep
# import glob
#
#
# def copyfile_slow(files, dest):
#     sleep(2)  # wait for main to set up everything
#     while bool(files):
#         file = files.pop()
#         shutil.copy(file, dest)
#         sleep(60)
#
#
# myPath = os.path.dirname(os.path.abspath(__file__))
# loc_test_data = os.path.join(os.path.abspath('.'), "tests/data")
# assert os.path.isdir(loc_test_data), logger.error("Test data is expected to be located at {}".format(loc_test_data))
# configurations.input_directory = '/tmp/test_mpiapp_input'
# configurations.output_directory = '/tmp/test_mpiapp_output'
# os.mkdir(configurations.input_directory)
# os.mkdir(configurations.output_directory)
# data = sorted(glob.glob1(loc_test_data, '*.tif'))
# data = [os.path.join(loc_test_data, item) for item in data]
# main_thread = Thread(target=main, kwargs={'conf': configurations, 'threads_stop': stop_event})
# main_thread.start()
# copyfile_slow(data, configurations.input_directory)
#
# while True:
#     if args.test:
#         print("Finished copying files to input directory.")
#         user_input = input("Do you want to delete input and output directories? (y/[n]):")
#         stop_event.set()
#         if user_input == 'y':
#             shutil.rmtree(configurations.input_directory)
#             shutil.rmtree(configurations.output_directory)
#             break
#         elif user_input == 'n':
#             break
#
#     elif not args.test:
#         user_input = input("Type 'quit' to stop processing:")
#         if user_input == "quit":
#             new_input = input("Do you want to quit? Any new incoming files will not be processed. (y/[n]):")
#             if new_input == "y":
#                 stop_event.set()
#                 break