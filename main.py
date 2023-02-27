from flask import Flask, jsonify
from db_connection import mydb
import datetime
import time


app = Flask(__name__)

start_time = datetime.time(9, 0, 0)
end_time = datetime.time(17, 0, 0)
current_time = datetime.datetime.now().time()


# To check how many persons logged in
@app.route('/api/v1/logged-persons', methods=['GET'])
def get_logged_persons():
    cursor = mydb.cursor()
    cursor.execute("select id, name, email from qc_persons where is_logged_in is true")
    logged_in_persons = cursor.fetchall()
    person_count = len(logged_in_persons)
    users_list = []

    for user in logged_in_persons:
        temp_user = {
            'id': user[0],
            'name': user[1],
            'email': user[2]
        }
        users_list.append(temp_user)

    if logged_in_persons:
        return jsonify(
            {'count': person_count,
             'result': users_list}
        )
    else:
        return jsonify({'message': 'No One Logged Yet'})


# To view the work status for each person
@app.route('/api/v1/logged-persons/<status>', methods=['GET'])
def get_person_work_status(status):
    if status == 'working':
        cursor = mydb.cursor()
        cursor.execute(
            "select qc_persons.id, qc_persons.name, qc_persons.email, tasks.id, tasks.task_name, tasks.description, "
            "tasks.status from qc_persons inner join tasks on qc_persons.id = tasks.assigned_person_id where "
            "tasks.status = 'Assigned' and is_logged_in is true"
        )
        working_persons = cursor.fetchall()
        count = len(working_persons)
        users_list = []

        for user in working_persons:
            temp_user = {
                'id': user[0],
                'name': user[1],
                'email': user[2],
                'task_id': user[3],
                'task_name': user[4],
                'task_description': user[5],
                'task_status': user[6]
            }
            users_list.append(temp_user)

        return jsonify({
            'count': count,
            'result': users_list
        })

    elif status == 'free':
        cursor = mydb.cursor()
        cursor.execute(
            "select id, name, email from qc_persons where is_logged_in is true and id not in (select qc_persons.id "
            "from qc_persons inner join tasks on qc_persons.id = tasks.assigned_person_id where tasks.status = "
            "'Assigned' and is_logged_in is true)"
        )
        free_persons = cursor.fetchall()
        count = len(free_persons)

        users_list = []

        for user in free_persons:
            temp_user = {
                'id': user[0],
                'name': user[1],
                'email': user[2]
            }
            users_list.append(temp_user)

        return jsonify({
            'count': count,
            'result': users_list
        })

    else:
        return jsonify({'message': 'Please give free or working at the end of URL'})


# For task assignment
@app.route('/api/v1/assign-task', methods=['PUT'])
def assign_task():
    # retrieve logged-in QC persons
    cursor = mydb.cursor()
    cursor.execute(
        "select id from qc_persons where is_logged_in is true and id not in (select qc_persons.id "
        "from qc_persons inner join tasks on qc_persons.id = tasks.assigned_person_id where tasks.status = "
        "'Assigned' and is_logged_in is true)"
    )
    free_persons = cursor.fetchall()

    # retrieve pending tasks
    cursor.execute("select id from tasks where status = 'Pending' and assigned_person_id is null")
    pending_tasks = cursor.fetchall()

    min_len = min(len(free_persons), len(pending_tasks))

    # if there are free persons, assign the pending task to them
    if free_persons:
        for i in range(min_len):
            cursor.execute("update Tasks set assigned_person_id = %s, status = 'Assigned' where id = %s",
                           (free_persons[i][0], pending_tasks[i][0]))

        mydb.commit()
        assigned_task = {'result': f'{min_len} tasks assigned successfully'}
        print(f'{min_len} tasks assigned successfully')

        return jsonify(assigned_task)

    else:
        print('No free QC person available')
        return jsonify({'message': 'QC persons are not available'})


@app.route('/api/v1/task-complete/<int:task_id>', methods=['PUT'])
def task_complete(task_id):
    cursor = mydb.cursor()
    cursor.execute("select status from tasks where id = %s;", (task_id,))
    task_status = cursor.fetchone()
    print(task_status)

    if not task_status:
        return jsonify({'message': 'There is no task available for this id'})

    elif task_status[0] == 'Assigned':
        cursor = mydb.cursor()
        cursor.execute("update tasks set status = 'Completed' where id = %s", (task_id,))
        mydb.commit()
        return jsonify({'message': 'Task completed successfully'})

    elif task_status[0] == 'Pending':
        return jsonify({'message': 'Task not yet assigned'})

    elif task_status[0] == 'Completed':
        return jsonify({'message': 'Task Already completed'})


@app.route('/api/v1/task-assign-job', methods=['PUT'])
def home():
    while True:
        if start_time <= current_time <= end_time:
            assign_task()
            time.sleep(60)

        else:
            time_to_wait = datetime.datetime.combine(datetime.datetime.today(), start_time) - datetime.datetime.today()
            time.sleep(time_to_wait.total_seconds())

            assign_task()
            time.sleep(60)


if __name__ == '__main__':
    app.run(debug=True)
