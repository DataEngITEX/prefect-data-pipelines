from prefect import flow,task

@task
def create_message():
    msg='Hello from Task '
    return(msg)
@flow
def something_else():
    result=30
    return result
@flow
def hello_world():
    task_message=create_message()
    sub_flow_message=something_else()
    new_message=task_message+str(sub_flow_message)
    print(new_message)

if __name__=="__main__":
    hello_world()