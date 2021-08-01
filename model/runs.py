from AutoMap import *

def get_pareto_weights():
    ret = []
    for i in np.arange(0, 1.1, 0.1):
        for j in np.arange(0, 1 - i, 0.1):
            ret += [(round(i, 2), round(j, 2), round(1 - i - j, 2))]

    return ret


if __name__ == '__main__':

    Session = sessionmaker(bind=engine)
    session = Session() 

    pareto_weights = get_pareto_weights()
    print(pareto_weights)

    # scenario_id = 0
    # baseline_id = 1

    # lambda_cost = 0.5
    # lambda_time = 0.25
    # lambda_co2e = 0.25

    # run = Runs(
    #     scenario_id = scenario_id,
    #     baseline_id = baseline_id,
    #     date = datetime.now(),
    #     description = 'first run',
    #     lambda_cost = lambda_cost,
    #     lambda_time = lambda_time,
    #     lambda_co2e = lambda_co2e 
    # )

    # session.add(run)
    # session.commit()