# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
from collect.cnrfc import cnrfc


def plot_ensemble_product_6(filename):
    """
    recreate CNRFC barchart figure from
    https://www.cnrfc.noaa.gov/ensembleProduct.php?id=XXXXX&prodID=6
    """
    response = cnrfc.get_ensemble_product_6('ORDC1')
    df = response['data']
    df.drop(['Avg', 'MP/Avg'], axis=0, inplace=True)
    df.loc[df.index!='90%'] = df.diff(-1)
    df.T[df.T.columns[::-1]].plot(kind='bar', 
                                  stacked=True, 
                                  color=['r', 'gold', 'lime', 'cyan', 'b'], 
                                  width=0.9)
    title_text = '\n'.join(['Monthly Volume Exceedance Values on the {}'.format(response['info']['title']),
                            'Latitude: {} Longitude: {}',
                            'Forecast for the period {} - {}',
                            'This is a conditional simulation based on the current conditions as of {}',
                            '(src=D)'])
    plt.title(title_text)
    plt.savefig(filename)