""" 
Classes for visualizing data analysis based upon the nature of the
machine learning model used.
"""

from dataclasses import dataclass
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd

import shap

@dataclass
class Plotter(object):
    
    def __post_init__(self):
        self.options = {'classifer' : ClassifierPlotter,
                        'linear' : LinearPlotter,
                        'grouper' : GrouperPlotter}
        self.x_options = {'full' : self.data.x,
                          'train' : self.data.x_train,
                          'test' : self.data.x_test,
                          'val' : self.data.x_val}
        self.y_options = {'full' : self.data.y,
                          'train' : self.data.y_train,
                          'test' : self.data.y_test,
                          'val' : self.data.y_val}
        return self
    
    def _pick_plotter(self, choice):
        return self.options[choice]

           
@dataclass        
class ClassifierPlotter(Plotter):
    
    settings : object
    export_path : str
    data : object
    data_to_use : str = 'full'
    
    def __post_init__(self):
        self.x = self.x_options[self.data_to_use]
        self.y = self.y_options[self.data_to_use]
        self.options = {'heat_map' : self.heat_map,
                        'summary' : self.summary,
                        'interactions' : self.interactions}
        shap.initjs()  
        return self
    
    def _compute_values(self, x, model):
        if model in ['xgb', 'random_forest']:
            self.explainer = shap.TreeExplainer(model.method)
            self.values = shap.TreeExplainer(model.method).shap_values(x)
        self.interaction_values = (self.explainer.shap_interaction_values(
                pd.DataFrame(x, columns = x.columns)))      
        return self
    
    def _one_cycle(self, plots, model, x = None):
        if not x:
            x = self.x
        self._compute_values(x, model)
        for plot in plots:
            self.options[plot](model = model, df = x) 
        return self      
        
    def dependency(self, model, var1, var2 = None, x = None, 
                   file_name = 'shap_dependency.png'):
        if var2:
            shap.dependence_plot(var1, 
                                 self.values, 
                                 x, 
                                 interaction_index = 'var2', 
                                 show = False)
        else:
            shap.dependence_plot(var1, 
                                 self.values, 
                                 x, 
                                 show = False)
        plt.savefig(os.path.join(self.export_path, file_name), 
                    bbox_inches='tight')
        plt.close()
        return self
    
    def heat_map(self, model, x = None, file_name = 'heat_map.png'):
        tmp = np.abs(self.interaction_values).sum(0)
        for i in range(tmp.shape[0]):
            tmp[i, i] = 0
        inds = np.argsort(-tmp.sum(0))[:50]
        tmp2 = tmp[inds,:][:,inds]
        plt.figure(figsize = (12, 12))
        plt.imshow(tmp2)
        plt.yticks(range(tmp2.shape[0]), 
                   x.columns[inds], 
                   rotation = 50.4, 
                   horizontalalignment = 'right')
        plt.xticks(range(tmp2.shape[0]), 
                   x.columns[inds], 
                   rotation = 50.4, 
                   horizontalalignment = 'left')
        plt.gca().xaxis.tick_top()
        plt.savefig(os.path.join(self.export_path, file_name), 
                    bbox_inches = 'tight')
        plt.close()
        return self
    
    def interactions(self, model, x = None, file_name = 'interactions.png', 
                     max_display = 20):
        shap.summary_plot(self.interaction_values, 
                          x, 
                          max_display = max_display,
                          show = False)
        plt.savefig(os.path.join(self.export_path, file_name), 
                    bbox_inches='tight')
        plt.close()
        return self
    
    def summary(self, model, x = None, file_name = 'shap_summary.png', 
                max_display = 20):
        shap.summary_plot(self.shap_values, 
                          x, 
                          max_display = max_display,
                          show = False)
        plt.savefig(os.path.join(self.export_path, file_name), 
                                 bbox_inches='tight')
        plt.close()
        return self

@dataclass
class LinearPlotter(Plotter):

    settings : object
    export_path : str
    data : object
    data_to_use : str = 'full'
      
    def __post_init__(self):
        self.x = self.x_options[self.data_to_use]
        self.y = self.y_options[self.data_to_use]
        return
    
@dataclass    
class GrouperPlotter(Plotter):

    settings : object
    export_path : str
    data : object
    data_to_use : str = 'full'
    
    def __post_init__(self):
        self.x = self.x_options[self.data_to_use]
        self.y = self.y_options[self.data_to_use]
        return