import matplotlib
import matplotlib.pyplot as plt
import numpy as np


def grouped_bar_plot():
    #observations
    n=4
    #labels /colors
    m=4

    labels = [f'L{i}' for i in range(n)]
    observation_label = [f'O{i}' for i in range(m)]

    data = np.random.randint(10,40, size=(m,n))

    width  = 1/(m)
    inner_margin = 0.10
    outer_margin = 0.3

    x = np.arange(0,len(labels),1+outer_margin)# the label locations

    _fig, ax = plt.subplots()
    for i in range(m):
        ax.bar(x+( (1-m)/2 + i)*width, data[i], width*(1-2*inner_margin), label=observation_label[i])

    ax.set_xticks(x)
    ax.set_xticklabels(labels)

    ax.set_ylabel('Scores')
    ax.set_title('Scores by group and gender')

    ax.legend()
    plt.savefig('../../data/plots/teste.pdf')
