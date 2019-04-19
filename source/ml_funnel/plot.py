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

from ml_funnel.methods import Methods

@dataclass
class Plotter(Methods):
    
    def __post_init__(self):
        super().__post_init__()
        return self
    
    def _check_plots(self):
        if not self.plots or self.plots == 'default':
            self.plots = self.defaults
        return self
    
    def apply(self, data, model, tube_num, splicer = ''):
        self.x, self.y = self._get_data(
                data = data, 
                data_to_use = self.settings['plot']['data_to_use'],
                train_test = False)
        self.model = model
        self.tube_num = tube_num
        self.splicer = splicer
        self._iter_plots()
        return self
    
    def _iter_plots(self):
        self._compute_values()
        self._check_plots()
        for plot in self.plots:
            self.options[plot]()     
        return self
        
    def save(self, file_name):
        export_path = self.filer._iter_path(model = self.model, 
                                            tube_num = self.tube_num, 
                                            splicer = self.splicer,
                                            file_name = file_name,
                                            file_type = 'png')
        plt.savefig(export_path, bbox_inches = 'tight')
        plt.close()
        return self
    
@dataclass        
class ClassifierPlotter(Plotter):
    
    plots : object = None
    use_results : object = False
    
    def __post_init__(self):
        super().__post_init__()
        self.options = {'heat_map' : self.heat_map,
                        'summary' : self.summary,
                        'interactions' : self.interactions}
        self.defaults = list(self.options.keys())
        self._check_plots()
        return self
    
    def _compute_values(self):
        if self.model.name in ['xgb', 'random_forest']:
            self.explainer = shap.TreeExplainer(self.model.method)
            self.values = shap.TreeExplainer(
                    self.model.method).shap_values(self.x)
        else:
            self.explainer = shap.KernelExplainer(self.model.method, self.x)
            self.values = shap.KernelExplainer(
                    self.model.method).shap_values(self.x)
        self.interaction_values = (self.explainer.shap_interaction_values(
                pd.DataFrame(self.x, columns = self.x.columns)))      
        return self
    
    def _one_cycle(self):
        self._compute_values(self.x, self.model)
        for plot in self.plots:
            self.options[plot]() 
        return self      
        
    def dependency(self, model, var1, var2 = None, x = None,
                   file_name = 'shap_dependency.png'):
        shap.initjs()  
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
        self.save(file_name)
        return self
    
    def heat_map(self, file_name = 'heat_map.png'):
        shap.initjs()  
        tmp = np.abs(self.interaction_values).sum(0)
        for i in range(tmp.shape[0]):
            tmp[i, i] = 0
        inds = np.argsort(-tmp.sum(0))[:50]
        tmp2 = tmp[inds,:][:,inds]
        plt.figure(figsize = (12, 12))
        plt.imshow(tmp2)
        plt.yticks(range(tmp2.shape[0]), 
                   self.x.columns[inds], 
                   rotation = 50.4, 
                   horizontalalignment = 'right')
        plt.xticks(range(tmp2.shape[0]), 
                   self.x.columns[inds], 
                   rotation = 50.4, 
                   horizontalalignment = 'left')
        plt.gca().xaxis.tick_top()
        self.save(file_name)
        return self
    
    def interactions(self, file_name = 'interactions.png',  max_display = 20):
        shap.initjs()  
        shap.summary_plot(self.interaction_values, 
                          self.x, 
                          max_display = max_display,
                          show = False)
        self.save(file_name)
        return self
    
    def summary(self, file_name = 'shap_summary.png', max_display = 20):
        shap.initjs()  
        shap.summary_plot(self.values, 
                          self.x, 
                          max_display = max_display,
                          show = False)
        self.save(file_name)
        return self

@dataclass
class LinearPlotter(Plotter):
    
    plots : object = None
         
    def __post_init__(self):
        super().__post_init__()
        self.options = {}
        self.defaults = list(self.options.keys())
        self._check_plots()
        return
    
@dataclass    
class GrouperPlotter(Plotter):
    
    plots : object = None
       
    def __post_init__(self):
        super().__post_init__()
        self.options = {}
        self.defaults = list(self.options.keys())
        self._check_plots()
        return