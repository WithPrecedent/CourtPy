"""
Class for creating dynamic pipelines for preprocessing, machine learning, 
and data analysis using a unified architecture.
"""
from dataclasses import dataclass
import os
import warnings

from ml_funnel.methods import Encoder, Grid, Interactor, Methods, Model
from ml_funnel.methods import Sampler, Scaler, Selector, Splicer, Splitter
#from ml_funnel.plot import Plotter
from ml_funnel.results import Results
from ml_funnel.settings import Settings

@dataclass   
class Funnel(object):
    
    data : object
    use_settings_file : bool = True
    settings = object = None
    col_groups : object = None 
    new_methods = object = None
        
    def __post_init__(self):    
        if self.use_settings_file:
            self.load_settings()
        self.data.seed = self.seed
        Methods.seed = self.seed
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
        self.data.split_xy(label = self.settings['funnel']['label'])
        Model.scale_pos_weight = (len(self.data.y.index) / 
                                  ((self.data.y == 1).sum())) - 1
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
        self.pipeline = self.settings['funnel']['pipeline']
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
        self.export_results = self.settings['results']['export']
        self.metrics = self.settings['results']['metrics']
        self.join_predictions = self.settings['results']['join_predictions']
        self.join_pred_probs = self.settings['results']['join_pred_probs']
        return self

    def add_method(self, step, name, method):
        self.steps[step].options.update({name : method})
        return self
    
    def create(self):
        if self.verbose:
            print('Creating all possible preprocessing pipelines')
        self.pipes = []
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
                                                     self.grid_params))                           
                                    self.pipes.append(tube)
        return self
        
    def _check_list(self, variable):
        if not variable:
            return ['none']
        elif isinstance(variable, list):
            return variable
        else:
            return [variable]         
 
    def iterate(self):
        self._one_loop()
        if self.splitter_params['val_size'] > 0:
            self._one_loop(use_val_set = True)
        return self
    
    def best_tube(self, results, metric):
        pipeline = results.table.loc[results.table[metric] 
                                    == results.table[metric].max()]
        tube = Tube(scaler = pipeline['scaler'],
                    splitter = pipeline['splitter'],
                    encoder = pipeline['encoder'],
                    interactor = pipeline['interactor'],
                    splicer = pipeline['splicer'],
                    sampler = pipeline['sampler'],
                    selector = pipeline['selector'],
                    model = pipeline['estimator'])
        return tube
    
    def _one_loop(self, use_val_set = False):
        for pipe in self.pipes:
            self.data.split_xy(label = self.settings['funnel']['label'])
            pipe.apply(self.data, use_val_set)
            self.results.add_result(pipe, use_val_set)
            del(pipe)
        return self

@dataclass 
class Tube(object):
    
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
    
    def apply(self, data, use_full_set = False, use_val_set = False):
        if self.scaler.name != 'none':
            data.x[data.num_cols] = (
                    self.scaler.apply(data.x[data.num_cols]))
        if self.splitter.name != 'none':
            data = self.splitter.apply(data)  
        if use_val_set:
            x_train = data.x_train
            y_train = data.y_train
            self.x_test = data.x_val
            self.y_test = data.y_val
        elif use_full_set:
            x_train = data.x_train
            y_train = data.y_train
            self.x_test = data.x_train
            self.y_test = data.y_train
        else:
            x_train = data.x_train
            y_train = data.y_train
            self.x_test = data.x_test
            self.y_test = data.y_test
        if self.encoder.name != 'none':
            self.encoder.fit(x_train, y_train)
            x_train = self.encoder.transform(x_train.reset_index(drop = True))
            self.x_test = self.encoder.transform(self.x_test.reset_index(
                    drop = True))
        if self.interactor.name != 'none':
            self.interactor.fit(x_train, y_train)
            x_train = self.interactor.transform(x_train.reset_index(
                    drop = True))
            self.x_test = self.interactor.transform(self.x_test.reset_index(
                    drop = True))
        if self.splicer.name != 'none':
            x_train = self.splicer.transform(x_train)
            self.x_test = self.splicer.transform(self.x_test)
        if self.sampler.name != 'none':
            self.sampler.fit(x_train, y_train)
            x_train = self.sampler.transform(x_train, y_train)
        if self.selector.name != 'none':
            self.selector.fit(x_train, y_train)
            x_train = self.selector.transform(x_train, y_train)
        if self.model.name != 'none':
            if self.model.use_grid and self.grid.name != 'none':
                self.grid.search(x_train, y_train)
                self.model.method = self.grid.best
            self.model.method.fit(x_train, y_train)  
        return self
        