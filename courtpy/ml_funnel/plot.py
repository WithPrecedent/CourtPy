""" 
Classes and functions for plotting based upon the nature and structure of the
machine learning model.
"""

from dataclasses import dataclass
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import shap

from ml_funnel.funnel import Tube

@dataclass
class Plotter(object):
    
    model : object
    data : object
    
    def __post_init__(self):
        if self.model.algorithm_type == 'classifier':
            if self.model.name in ['xgb', 'random_forest']:
                self.method = TreePlotter
            else:
                self.method = ClassifierPlotter
        elif self.model.algorithm_type == 'regressor':
            self.method = LinearPlotter
        elif self.model.algorithm_type == 'grouper':
            self.method = GrouperPlotter
        return
    
    def convert_results(self, results, row_num = None, metric = None):
        if metric:
            pipeline = results.table.loc[results.table[metric] 
                                         == results.table[metric].max()]
        elif row_num:
            pipeline = results.table.iloc[row_num]
        pipeline = pipeline[results.step_columns]
        self.tube = Tube(scaler = pipeline['scaler'],
                         splitter = pipeline['splitter'],
                         encoder = pipeline['encoder'],
                         interactor = pipeline['interactor'],
                         splicer = pipeline['splicer'],
                         sampler = pipeline['sampler'],
                         selector = pipeline['selector'],
                         model = pipeline['estimator'])
        return self

@dataclass        
class TreePlotter(Plotter):
    
    method : object
    x : object
    
    def __post_init__(self):
        shap.initjs()
        self.explainer = shap.TreeExplainer(self.method)
        self.values = shap.TreeExplainer(self.method).shap_values(self.x)
        self.interaction_values = (self.explainer.shap_interaction_values(
                pd.DataFrame(self.x, columns = self.x.columns)))
        return self
    
    def dependency(self, export_path, var1, var2 = None):
        if var2:
            shap.dependence_plot(var1, self.values, self.x, 
                                 interaction_index = 'var2', show = False)
        else:
            shap.dependence_plot(var1, self.shap_values, self.x, show = False)
            plt.savefig(export_path, bbox_inches='tight')
            plt.close()
        return self
    
    def heat_map(self, export_path):
        tmp = np.abs(self.interaction_values).sum(0)
        for i in range(tmp.shape[0]):
            tmp[i, i] = 0
        inds = np.argsort(-tmp.sum(0))[:50]
        tmp2 = tmp[inds,:][:,inds]
        plt.figure(figsize = (12, 12))
        plt.imshow(tmp2)
        plt.yticks(range(
                tmp2.shape[0]), self.x.columns[inds], rotation = 50.4, 
                   horizontalalignment = 'right')
        plt.xticks(range(
                tmp2.shape[0]), self.x.columns[inds], rotation = 50.4, 
                   horizontalalignment = 'left')
        plt.gca().xaxis.tick_top()
        plt.savefig(export_path, bbox_inches = 'tight')
        plt.show()
        plt.close()
        return self
    
    def interactions(self, export_path, max_display = 20):
        shap.summary_plot(self.interaction_values, self.x, max_display = max_display,
                          show = False)
        plt.savefig(export_path, bbox_inches='tight')
        plt.close()
        return self
    
    def summary(self, export_path, max_display = 20):
        shap.summary_plot(self.shap_values, self.x, max_display = max_display,
                          show = False)
        plt.savefig(export_path, bbox_inches='tight')
        plt.close()
        return self

@dataclass
class LinearPlotter(Plotter):
 
    method : object
    bunch : object
      
    def __post_init__(self):
        
        return

@dataclass    
class ClassifierPlotter(Plotter):

    method : object
    bunch : object
      
    def __post_init__(self):
        
        return
    
@dataclass    
class GrouperPlotter(Plotter):

    method : object
    bunch : object
      
    def __post_init__(self):
        
        return