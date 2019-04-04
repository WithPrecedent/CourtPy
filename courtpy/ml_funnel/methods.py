"""
Parent and child clases for implementing the different steps in the 
ml_funnel pipeline.
"""
from dataclasses import dataclass
import os
import pickle
from scipy.stats import randint, uniform
from sklearn.cluster import AffinityPropagation, Birch, KMeans
from sklearn.ensemble import AdaBoostClassifier, AdaBoostRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import BayesianRidge, Lasso, LassoLars
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from sklearn.preprocessing import MaxAbsScaler, MinMaxScaler, Normalizer
from sklearn.preprocessing import QuantileTransformer, RobustScaler
from sklearn.preprocessing import StandardScaler
from sklearn.svm import LinearSVC, SVC

from category_encoders import BackwardDifferenceEncoder, BaseNEncoder
from category_encoders import BinaryEncoder, HashingEncoder, HelmertEncoder 
from category_encoders import LeaveOneOutEncoder, OneHotEncoder
from category_encoders import OrdinalEncoder, SumEncoder, TargetEncoder
from category_encoders import PolynomialEncoder
from imblearn.combine import SMOTEENN, SMOTETomek
from imblearn.over_sampling import ADASYN, SMOTE
from prince import CA, FAMD, MCA, MFA, PCA
from xgboost import XGBClassifier, XGBRegressor

#from ml_funnel.logit_encode import LogitEncoder
    
@dataclass
class Methods(object):  
    """
    Parent class for preprocessing and modeling methods in the ml_funnel. 
    The Methods class allows for shared initialization, loading, and saving 
    methods to be accessed by all child classes. 
    The 'use' method applies sklearn fit and transform methods for transformers 
    that are wholly compatible with sklearn. For non-compatible parts of the 
    funnel, specific 'use methods are included with the child classes.
    """

    def __post_init__(self):     
        return self
    
    def _check_params(self):
        if not self.params:
            self.params = self.defaults
        return self
    
    def initialize(self):
        self._check_params()
        if self.runtime_params:
            self.params.update(self.runtime_params)
        if self.name != 'none':
            self.method = self.options[self.name]
            if self.params:
                self.method = self.method(**self.params)
            else:
                self.method = self.method()
        return self
    
    def apply(self, x, y = None):
        if self.name != 'none':
            self.method.fit(x, y)
            x = self.method.transform(x)
        return x
    
    def fit(self, x, y):
        return self.method.fit(x, y)
                               
    def transform(self, x):                        
        return self.method.transform(x) 
    
    def fit_transform(self, x, y):
        self.fit(x, y)
        return self.transform(x)
    
    def load(self, import_path, name, prefix = ''):
        if self.verbose:
            print('Importing', name)
        if prefix:
            import_file = prefix + '_' + name + '.pkl' 
        else:
            import_file = name + '.pkl' 
        self.name = name
        import_path = os.path.join(self.import_path, import_file)
        self.method = pickle.load(open(import_path, 'rb'))
        return self
    
    def save(self, export_path, name, prefix = ''):
        if self.verbose:
            print('Exporting', name)
        if prefix:
            export_file = prefix + '_' + name + '.pkl' 
        else:
            export_file = name + '.pkl' 
        export_path = os.path.join(self.export_path, export_file)
        pickle.dump(self.method, open(export_path, 'wb'))
        return self

@dataclass
class Scaler(Methods):
    
    name : str = ''
    params : object = None
    
    def __post_init__(self):
        self.options = {'maxabs' : MaxAbsScaler,
                        'minmax' : MinMaxScaler,
                        'normalizer' : Normalizer,
                        'quantile' : QuantileTransformer,
                        'robust' : RobustScaler,
                        'standard' : StandardScaler}
        self.defaults = {'copy' : False}
        self.runtime_params = {}
        self.initialize()
        return self

@dataclass
class Splitter(Methods):

    name : str = ''
    params : object = None
    create_val_set : bool = False
    
    def __post_init__(self):
        self.options = {'train_test' : self._split_data,
                        'train_test_val' : self._split_data,
                        'cv' : self._cv_split,
                        'none' : self._no_split}
        self.defaults = {'test_size' : 0.33,
                         'val_size' : 0,
                         'kfolds' : 5,
                         'krepeats' : 10}       
        self._check_params()
        self.method = self.options[self.name]
        self.params.update({'random_state' : self.seed})
        return self
    
    def _cv_split(self, data):
#        for train_index, test_index in self.params['folder'].split(data.x, data.y):
#            data.x_train, data.x_test = (data.x.iloc[train_index], 
#                                         data.x.iloc[test_index])
#            data.y_train, data.y_test = (data.y.iloc[train_index], 
#                                         data.y.iloc[test_index])
        return data
    
    def _no_split(self, data):
        return data
        
    def _split_data(self, data):
        data.x_train, data.x_test, data.y_train, data.y_test = (
                self._one_split(data.x, data.y, self.params['test_size']))
        if self.create_val_set:
            data.x_train, data.x_val, data.y_train, data.y_val = (
                self._one_split(data.x_train, data.y_train,
                                self.params['val_size']))
        return data
    
    def _one_split(self, x, y, split_size):
        x_train, x_test, y_train, y_test = (
                train_test_split(x, y, 
                                 random_state = self.seed, 
                                 test_size = split_size))
        return x_train, x_test, y_train, y_test
    
    def apply(self, data):
        return self.method(data)       
   
@dataclass
class Encoder(Methods):
    
    name : str = ''
    params : object = None
    columns : object = None
    
    def __post_init__(self):
        self.options = {'backward' : BackwardDifferenceEncoder,
                        'basen' : BaseNEncoder,
                        'binary' : BinaryEncoder,
                        'hashing' : HashingEncoder,
                        'helmert' : HelmertEncoder,
#                        'logit' : LogitEncoder,
                        'loo' : LeaveOneOutEncoder,
                        'dummy' : OneHotEncoder,
                        'ordinal' : OrdinalEncoder,
                        'sum' : SumEncoder,
                        'target' : TargetEncoder}
        self.defaults = {}
        self.runtime_params = {'cols' : self.columns}
        self.initialize()
        return self
    
    def fit(self, x, y):
        return self.method.fit(x, y)
                               
    def transform(self, x):
        x = self.method.transform(x)
        for col in self.columns:
            x[col] = x[col].astype(float, copy = False)                   
        return x
    
@dataclass
class Interactor(Methods):
    
    name : str = ''
    params : object = None
    columns : object = None
    
    def __post_init__(self):
        self.options = {'polynomial' : PolynomialEncoder}
        self.defaults = {}
        self.runtime_params = {'cols' : self.columns}
        self.initialize()
        return self

@dataclass
class Splicer(Methods):
    
    name : str = ''
    options : object = None
    include_all : bool = True
    
    def __post_init__(self):
        if self.include_all:
            self.test_columns = []
            for group, columns in self.options.items():
                self.test_columns.extend(columns)
            self.options.update({'all' : self.test_columns})
        self.method = self.options[self.name]
        return self
        
    def fit(self, x, y):
        return self
    
    def transform(self, x):
        if not isinstance(self.method, list):
           self.method = [self.method]
        drop_list = [i for i in self.test_columns if i not in self.method]
        for col in drop_list:
            if col in x.columns:
                x.drop(col, axis = 'columns', inplace = True)
        return x

@dataclass
class Sampler(Methods):
    
    name : str = ''
    params : object = None
    
    def __post_init__(self):
        self.options = {'adasyn' : ADASYN,
                        'smote' : SMOTE,
                        'smoteenn' :  SMOTEENN,
                        'smotetomek' : SMOTETomek}
        self.defaults = {}
        self.runtime_params = {'random_state' : self.seed}
        self.initialize()
        return self

@dataclass
class Selector(Methods):
    
    name : str = ''
    params : object = None
    
    def __post_init__(self):
        self.options = {'famd' : FAMD, 
                        'mca' : MCA, 
                        'mfa' : MFA, 
                        'pca' : PCA,
                        'ca' : CA}
        self.defaults = {'n_components' : 10,
                         'n_iter' : 10,
                         'copy' : False,
                         'check_input' : True,
                         'engine' : 'auto'}
        self.runtime_params = {'random_state' : self.seed}
        self.initialize()
        return self
    
@dataclass
class Model(Methods):
    
    name : str
    algorithm_type : bool
    params : object
    use_gpu : bool = False
    scale_pos_weight : int = 1
    use_grid : bool = False
    
    def __post_init__(self):
        if self.algorithm_type == 'classifier':
            self.options = {'ada' : AdaBoostClassifier,
                            'logit' : LogisticRegression,
                            'random_forest' : RandomForestClassifier,
                            'svm_linear' : LinearSVC,
                            'svm' : SVC,
                            'xgb' : XGBClassifier}   
        elif self.algorithm_type == 'regressor':
            self.options = {'ada' : AdaBoostRegressor,
                            'bayes_ridge' : BayesianRidge,
                            'lasso' : Lasso,
                            'lasso_lars' : LassoLars,
                            'ols' : LinearRegression,
                            'random_forest' : RandomForestRegressor,
                            'ridge' : Ridge,
                            'xgb' : XGBRegressor}    
        elif self.algorithm_type == 'grouper':
            self.options = {'affinity' : AffinityPropagation,
                            'birch' : Birch,
                            'kmeans' : KMeans}
        self._parse_params()
        self.runtime_params = {'random_state' : self.seed}
        if self.name == 'xgb' and self.use_gpu:     
            self.runtime_params.update({'tree_method' : 'gpu_exact'})
        self.initialize()
        return self
    
    def _parse_params(self):
        self.grid = {}
        new_params = {}
        for param, values in self.params.items():
            if isinstance(values, list):              
                self.use_grid = True
                if isinstance(values[0], float):
                    self.grid.update({param : uniform(values[0], values[1])})
                elif isinstance(values[0], int):
                    self.grid.update({param : randint(values[0], values[1])})
            else:
                new_params.update({param : values})
        self.params = new_params
        if self.use_grid and self.name == 'xgb':  
            self.grid.update({'scale_pos_weight' : 
                              uniform(self.scale_pos_weight / 1.5, 
                              self.scale_pos_weight * 1.5)})
        elif self.name == 'xgb':
            self.params.update({'scale_pos_weight' : self.scale_pos_weight})
        return self

@dataclass    
class Grid(Methods):
    
    name : str
    model : object
    params : object
    
    def __post_init__(self):
        self.options = {'random' : RandomizedSearchCV,
                        'fixed' : GridSearchCV,
                        'bayes' : None}
        self.runtime_params = {'estimator' : self.model.method,
                               'param_distributions' : self.model.grid,
                               'scoring' : self.params['scoring'],
                               'refit' : self.params['scoring'][0],
                               'random_state' : self.seed}
        self.params.update(self.runtime_params)
        self.method = self.options[self.name](**self.params)
        return self  
    
    def search(self, x, y):
        self.method.fit(x, y)
        self.best = self.method.best_estimator_
        print('The score of the best estimator for the ' + self.model.name
              + ' model is ' + str(self.method.best_score_))
        return self
    