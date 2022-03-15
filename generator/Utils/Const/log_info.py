#modify the parameter settings
log_transactions={
 "study_hours": {"A": {"range": [0.8,0.9],"pr": [0.2,0.8],"loc": 80,"scale": 30},
                 "B": {"range": [0.7,0.8,0.9],"pr": [0.2,0.5,0.3],"loc": 70,"scale": 20},
                 "C": {"range": [0.6,0.7,0.8,0.9],"pr": [0.3,0.4,0.2,0.1],"loc": 40,"scale": 30},
                 "D": {"range": [0.4,0.5,0.6,0.7],"pr": [0.2,0.5,0.2,0.1],"loc": 30,"scale": 20},
                 "E": {"range": [0.4,0.5,0.6,0.7],"pr": [0.4,0.4,0.15,0.05],"loc": 40,"scale": 20}},
 "during_event": {"A": {"poisson_start": 0.1,"poisson_end": 0.3,"mu": 3,"loc": 1,"percent": 0.95,"freq": 1},
                  "B": {"poisson_start": 0.1,"poisson_end": 0.4,"mu": 3,"loc": 1,"percent": 0.8,"freq": 2},
                  "C": {"poisson_start": 0.1,"poisson_end": 0.5,"mu": 5,"loc": 2,"percent": 0.75,"freq": 3},
                  "D": {"poisson_start": 0.1,"poisson_end": 0.8,"mu": 3,"loc": 2,"percent": 0.65,"freq": 4},
                  "E": {"poisson_start": 0.3,"poisson_end": 1,"mu": 5,"loc": 1,"percent": 0.5,"freq": 5} },
 "event_after": {"A": {"poisson_start": 0.1,"poisson_end": 0.3,"mu": 3,"loc": 0,"percent": 0.95,"freq": 1},
                 "B": {"poisson_start": 0.1,"poisson_end": 0.4,"mu": 3,"loc": 1,"percent": 0.9,"freq": 2},
                 "C": {"poisson_start": 0.1,"poisson_end": 0.6,"mu": 4,"loc": 1,"percent": 0.8,"freq": 3},
                 "D": {"poisson_start": 0.1,"poisson_end": 0.8,"mu": 3,"loc": 2,"percent": 0.7,"freq": 4},
                 "E": {"poisson_start": 0.3,"poisson_end": 1,"mu": 4,"loc": 2,"percent": 0.6,"freq": 5}},
 "first_login": {"A": {"poisson_start": 0.1,"poisson_end": 0.3,"mu": 3,"loc": 0},
                 "B": {"poisson_start": 0.1,"poisson_end": 0.4,"mu": 3,"loc": 1},
                 "C": {"poisson_start": 0.1,"poisson_end": 0.5,"mu": 5,"loc": 2},
                 "D": {"poisson_start": 0.1,"poisson_end": 0.8,"mu": 3,"loc": 2},
                 "E": {"poisson_start": 0.3,"poisson_end": 1,"mu": 5,"loc": 1}}
}

