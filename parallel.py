import multiprocessing
import time
from rockset import Client, Q, F, P, ParamDict
rs = Client()





def audio_Stage1( process_name, tasks, results ):

	print('[%s] evaluation routine starts' % process_name)

	qlambda = rs.QueryLambda.retrieveByVersion(
	    'all_audios_stage1',
	    version = 'c728db03ee4e3c50',
	    #version= '3064713fedc5e63a' , #version2
	   # '80cb8e8a16cafb5c', for. recall_ads collections
	    workspace='commons')

	params = ParamDict()
	overlap = 5000

	while True : 

		task_present = tasks.get()

		if (isinstance(task_present, tuple)) == False :

			print('[%s] evaluation routine quits' % process_name)
			# Indicate finished
			results.put(-1)
			break

		else:

			params['interval'] = 5000

			params['object_id'] = task_present[0]
			params['start_time'] =  task_present[1]
			maxtime = task_present[2]

			time_start = time.time()

			while  params['start_time'] < maxtime:

				result = qlambda.execute(parameters=params).results

				if params['start_time'] % 500000 == 0: 
					print((task_present[0]) + ' - start time - '+ str(params['start_time']))

				params['start_time'] = params['start_time'] + overlap
				print(result, params)

				if len(result) > 0:
					ret = rs.Collection.add_docs('Audio_Stage1_v3', result)

			time_stop = time.time()
			exec_time = time_stop - time_start
			results.put( exec_time )




# define worker function
# def calculate(process_name, tasks, results):
#     print('[%s] evaluation routine starts' % process_name)

#     while True:
#         new_value = tasks.get()
#         if new_value < 0:
#             print('[%s] evaluation routine quits' % process_name)

#             # Indicate finished
#             results.put(-1)
#             break
#         else:
#             # Compute result and mimic a long-running task
#             compute = new_value * new_value
#             sleep(0.02*new_value)

#             # Output which process received the value
#             # and the calculation result
#             print('[%s] received value: %i' % (process_name, new_value))
#             print('[%s] calculated value: %i' % (process_name, compute))

#             # Add result to the queue
#             results.put(compute)
#     return


if __name__ == "__main__":
    # Define IPC manager
    manager = multiprocessing.Manager()

    # Define a list (queue) for tasks and computation results
    tasks = manager.Queue()
    results = manager.Queue()


num_processes = 4
pool = multiprocessing.Pool(processes=num_processes)
processes = []


# Initiate the worker processes
for i in range(num_processes):

    # Set process name
    process_name = 'P%i' % i

    # Create the process, and connect it to the worker function
    new_process = multiprocessing.Process(target=audio_Stage1, args=(process_name,tasks,results))

    # Add new process to the list of processes
    processes.append(new_process)

    # Start the process
    new_process.start()


# Fill task queue
task_list = [('LRW-10212020-225000', 76000, 1384000), ('LRW-10212020-225000', 2158000, 3258000), ('LRW-10212020-225000', 5485000, 5665000),('LRW-10212020-225000', 6190000, 7354000)]
 # 1, 780, 256, 142, 68, 183, 334, 325, 3]


for single_task in task_list:
    tasks.put(single_task)

# Wait while the workers process
# sleep(5)


# Quit the worker processes by sending them -1
for i in range(num_processes):
    tasks.put(-1)

# Read calculation results
num_finished_processes = 0
while True:
    # Read result
    new_result = results.get()

    # Have a look at the results
    if new_result == -1:
        # Process has finished
        num_finished_processes += 1

        if num_finished_processes == num_processes:
            break
    else:
        # Output result
        print('Result:' + str(new_result))    