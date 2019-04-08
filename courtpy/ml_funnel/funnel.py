"""
Class for creating dynamic test tubes for preprocessing, machine learning, 
and data analysis using a unified architecture.
"""
from dataclasses import dataclass
import os
import pickle
import warnings

from ml_funnel.methods import Encoder, Grid, Interactor, Methods, Model
from ml_funnel.methods import Sampler, Scaler, Selector, Splicer, Splitter
from ml_funnel.plot import ClassifierPlotter, GrouperPlotter, LinearPlotter   
from ml_funnel.results import Results
from ml_funnel.settings import Settings

@dataclass   
class Funnel(object):
    
    data : object
    use_settings_file : bool = True
    settings : object = None
    col_groups : object = None 
    new_methods : object = None
    best : object = None
        
    def __post_init__(self):    
        if self.use_settings_file:
            self.load_settings()
        Methods.settings = self.settings 
        Methods.seed = self.seed
        self.data.seed = self.seed 
        if not self.pandas_warnings:
            warnings.filterwarnings('ignore')
        if self.new_methods:
            for step, method_dict in self.new_methods.items():
                for key, value in method_dict.items():
                    self.add_method(step, key, value)
        if self.data.splice_options:
            self.splicers = list(self.data.splice_options.keys())
        else:
            self.splicers = []
        self.results = Results(data = self.data, 
                               settings = self.settings['results'],
                               model_type = self.algorithm_type,
                               verbose = self.verbose)
        self.data.split_xy(label = self.label)
        if self.compute_scale_pos_weight:
            Model.scale_pos_weight = (len(self.data.y.index) / 
                                      ((self.data.y == 1).sum())) - 1
        if self.plots:
            if self.algorithm_type == 'classifier':
                self.plotter = ClassifierPlotter
            elif self.algorithm_type == 'regressor':
                self.plotter = LinearPlotter    
            elif self.algorithm_type == 'grouper':
                self.plotter = GrouperPlotter
        else:
            self.plotter = None
        return self
        
    def load_settings(self, import_path = ''):
        if import_path:
            self.settings = Settings(import_path)
        else:
            self.settings = Settings(os.path.join('ml_funnel', 
                                                  'ml_settings.ini'))
        self.verbose = self.settings['defaults']['verbose']
        self.pandas_warnings = self.settings['defaults']['warnings']
        self.gpu = self.settings['defaults']['gpu']
        self.seed = self.settings['defaults']['seed']
        self.file_encoding = self.settings['files']['encoding']
        self.boolean_out = self.settings['files']['boolean_out']
        self.file_format_in = self.settings['files']['data_in']
        self.file_format_out = self.settings['files']['data_out']
        self.output_folder = self.settings['files']['output_folder']
        if not os.path.exists(self.output_folder):
                os.makedirs(self.output_folder)
        self.scalers = self.settings['funnel']['scaler']
        self.scaler_params = self.settings['scaler_params']
        self.splitter = self.settings['funnel']['splitter']
        self.splitter_params = self.settings['splitter_params']
        self.encoders = self.settings['funnel']['encoder']
        self.encoder_params = self.settings['encoder_params']
        self.interactors = self.settings['funnel']['interactor']
        self.interactor_params = self.settings['interactor_params']
        self.use_splicers = self.settings['funnel']['splicer']
        self.include_all = self.settings['splicer_params']['include_all']
        self.samplers = self.settings['funnel']['sampler']
        self.sampler_params = self.settings['sampler_params']
        self.selectors = self.settings['funnel']['selector']
        self.selector_params = self.settings['selector_params']
        self.grid_search = self.settings['funnel']['grid_search']
        self.search_method = self.settings['funnel']['search_method']
        self.grid_params = self.settings['grid_params']
        self.export_methods = self.settings['funnel']['export_methods']
        self.algorithm_type = self.settings['funnel']['algorithm_type']
        self.algorithms = self.settings['funnel']['algorithms']
        self.label = self.settings['funnel']['label']
        self.plots = self.settings['funnel']['plots']
        self.compute_scale_pos_weight = (
                self.settings['funnel']['compute_scale_pos_weight'])
        self.export_results = self.settings['results']['export']
        self.model_data_to_use = self.settings['funnel']['data_to_use']
        self.metrics = self.settings['results']['metrics']
        self.key_metric = self._check_list(self.metrics)[0]
        self.join_predictions = self.settings['results']['join_predictions']
        self.join_pred_probs = self.settings['results']['join_pred_probs']
        self.plot_folder = self.settings['plot']['subfolder']
        self.plot_data_to_use = self.settings['plot']['data_to_use']
        return self

    def add_method(self, step, name, method):
        self.steps[step].options.update({name : method})
        return self
    
    def create(self):
        if self.verbose:
            print('Creating all possible preprocessing tubelines')
        self.tubes = []
        if self.include_all:
            self.splicers.append('all')
        for scaler in self._check_list(self.scalers):
            for encoder in self._check_list(self.encoders):
                for interactor in self._check_list(self.interactors):
                    for splicer in self._check_list(self.splicers): 
                        for sampler in self._check_list(self.samplers):
                            for selector in self._check_list(self.selectors):
                                for algorithm in self._check_list(
                                        self.algorithms):
                                    model_params = (
                                        self.settings[algorithm + '_params'])
                                    model = Model(algorithm,
                                                  self.algorithm_type,
                                                  model_params)
                                    tube = Tube(Scaler(scaler, 
                                                       self.scaler_params),
                                                Splitter(self.splitter,
                                                         self.splitter_params),
                                                Encoder(encoder,
                                                        self.encoder_params, 
                                                        self.data.cat_cols),
                                                Interactor(interactor,
                                                    self.interactor_params,
                                                    self.data.interact_cols),
                                                Splicer(splicer,
                                                    self.data.splice_options,
                                                    self.include_all),
                                                Sampler(sampler,
                                                        self.sampler_params),
                                                Selector(selector,
                                                         self.selector_params),
                                                model,
                                                Grid(self.search_method,
                                                     model,
                                                     self.grid_params),
                                                self.plotter(
                                                        export_path = ( 
                                                            self.output),
                                                        plots = self.plots))                          
                                    self.tubes.append(tube)
        return self
        
    def _check_list(self, variable):
        if not variable:
            return ['none']
        elif isinstance(variable, list):
            return variable
        else:
            return [variable]         
 
    def iterate(self):
        self.best = None
        self._one_loop()
        if self.splitter_params['val_size'] > 0:
            self._one_loop(use_val_set = True)
        self.plotter(data = self.data,
                     settings = self.settings,
                     export_path = self.plot_path,
                     data_to_use = self.data_to_use)
        return self
    
    def _one_loop(self, use_val_set = False):
        for tube in self.tubes:
            self.data.split_xy(label = self.settings['funnel']['label'])
            tube.apply(self.data, use_val_set)
            self.results.add_result(tube, use_val_set)
            if not self.best:
                self.best = tube
                self.best_score = self.results.table.loc[
                        self.results.table.index[-1], self.key_metric]
            elif (self.results.table.loc[self.results.table.index[-1], 
                                         self.key_metric] > self.best_score):
                self.best = tube
                self.best_score = self.results.table.loc[
                        self.results.table.index[-1], self.key_metric]
            del(tube)
        return self
    
    def add_plot(self, name, method):
        self.plotter.options.update({name : method})
        return self
    
    def visualize(self, tube = None, funnel = None):
        if tube:
            self._visualize_tube(tube)
        else:
            for tube in funnel.pipes:
                self._visualize_tube(tube)              
        return self
    
    def _visualize_tube(self, tube):
        if self.visuals == 'default':
            plots = list(self.plotter.options.keys())
        else:
            plots = self._check_list(self.visuals)
        self.plotter._one_cycle(plots = plots, 
                                model = tube.model)
        return self
    
    @staticmethod
    def _file_name_join(i_path, file_name, file_format):
        return ''.join(os.path.join(i_path, file_name), '.csv')
    
    def save_everything(self, export_path):
        self.save_funnel(export_path)
        self.save_results(export_path)
        if self.best:
            self.save_tube(export_path, self.best)
        return self
    
    def load_funnel(self, import_path, return_funnel = False):
        tubes = pickle.load(open(import_path, 'rb'))
        if return_funnel:
            return tubes
        else:
            self.tubes = tubes
            return self
    
    def save_funnel(self, export_path):
        pickle.dump(self.tubes, open(export_path, 'wb'))
        return self
    
    def load_tube(self, import_path):
        tube = pickle.load(open(import_path, 'rb'))
        return tube
    
    def save_tube(self, export_path, tube):
        pickle.dump(tube, open(export_path, 'wb'))
        return self
    
    def load_results(self, import_path, file_name = 'results_table',
                     file_format = 'csv', encoding = 'windows-1252', 
                     float_format = '%.4f', message = 'Exporting results',
                     return_results = False):
        results_path = self._file_name_join(import_path,
                                            file_name,
                                            file_format)
        results = self.data.load(import_path = results_path,
                                 encoding = encoding,
                                 float_format = float_format,
                                 message = 'Importing results')      
        if return_results:
            return results
        else:
            self.results = results
            return self
    
    def save_results(self, export_path, file_name = 'results_table',
                     file_format = 'csv', encoding = 'windows-1252', 
                     float_format = '%.4f', message = 'Exporting results'):
        results_path = self._file_name_join(export_path,
                                            file_name,
                                            file_format)
        self.data.save(df = self.results.table, 
                       export_path = results_path,
                       encoding = encoding,
                       float_format = float_format,
                       message = 'Exporting results')
        return self

@dataclass 
class Tube(Methods):
    
    scaler : object = None
    splitter : object = None
    encoder : object = None
    interactor : object = None
    splicer : object = None
    sampler : object = None
    selector : object = None
    model : object = None
    grid : object = None
    plotter : object = None
    settings : object = None
    
    def apply(self, data, use_full_set = False, use_val_set = False):
        self.data = data
        if self.scaler.name != 'none':
            self.data.x[self.data.num_cols] = (
                    self.scaler.apply(self.data.x[self.data.num_cols]))
        if self.splitter.name != 'none':
            self.data = self.splitter.apply(self.data)  
        if use_val_set:
            (self.data.x_train, self.data.y_train, self.data.x_test, 
             self.data.y_test) = (
                    self._get_data(data = data, data_to_use = 'val', 
                                   train_test = True))
        elif use_full_set:
            (self.data.x_train, self.data.y_train, self.data.x_test, 
             self.data.y_test) = (
                    self._get_data(data = data, data_to_use = 'full', 
                                   train_test = True))
        else:
            (self.data.x_train, self.data.y_train, self.data.x_test, 
             self.data.y_test) = (
                    self._get_data(data = data, data_to_use = 'test', 
                                   train_test = True))
        if self.encoder.name != 'none':
            self.encoder.fit(self.data.x_train, self.data.y_train)
            self.data.x_train = (
                    self.encoder.transform(self.data.x_train.reset_index(
                    drop = True)))
            self.data.x_test = (
                    self.encoder.transform(self.data.x_test.reset_index(
                    drop = True)))
            self.data.x = (self.encoder.transform(self.data.x.reset_index(
                    drop = True)))
        if self.interactor.name != 'none':
            self.interactor.fit(self.data.x_train, self.data.y_train)
            self.data.x_train = (
                    self.interactor.transform(self.data.x_train.reset_index(
                    drop = True)))
            self.data.x_test = (
                    self.interactor.transform(self.data.x_test.reset_index(
                    drop = True)))
            self.data.x = (
                    self.interactor.transform(self.data.x.reset_index(
                    drop = True)))
        if self.splicer.name != 'none':
            self.data.x_train = self.splicer.transform(self.data.x_train)
            self.data.x_test = self.splicer.transform(self.data.x_test)
        if self.sampler.name != 'none':
            self.sampler.fit(self.data.x_train, self.data.y_train)
            self.data.x_train = (
                    self.sampler.transform(
                            self.data.x_train, self.data.y_train))
        if self.selector.name != 'none':
            self.selector.fit(self.data.x_train, self.data.y_train)
            self.data.x_train = (
                    self.selector.transform(
                            self.data.x_train, self.data.y_train))
        if self.model.name != 'none':
            if self.model.use_grid and self.grid.name != 'none':
                self.grid.search(self.data.x_train, self.data.y_train)
                self.model.method = self.grid.best
            self.model.method.fit(self.data.x_train, self.data.y_train)  
        if self.plotter:
            self.plotter.apply(data = self.data, model = self.model)
        return self
        