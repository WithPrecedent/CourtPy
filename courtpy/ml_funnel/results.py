"""
Class and methods for ml_funnel which creates a results table and other 
general scorers/metrics for machine learning based upon the type of model
used. Users can manually add metrics not already included in the metrics
dictionary by passing them to add_metric.
"""
from dataclasses import dataclass
import pandas as pd
import sklearn.metrics as met

#import eli5

from ml_funnel.methods import Methods

@dataclass  
class Results(Methods):
    """
    Class for storing machine learning experiment results.
    """    
    data : object = None
    settings : object = None
    algorithm_type : str = 'classifier'
    verbose : bool = True
    
    def __post_init__(self):
        self.step_columns = ['tube_number', 'predictors', 'scaler', 'splitter', 
                             'splicer', 'encoder', 'interactor', 'sampler', 
                             'selector', 'estimator', 'seed', 'validation_set']
        self.columns = self.step_columns
        self.columns.extend(self.settings['metrics']) 
        self.table = pd.DataFrame(columns = self.columns)
        return self
    
    @staticmethod
    def _check_none(step):
        """
        Checks if metric listed is either 'none' or 'all.' Otherwise, it
        returns the name of the method selected.
        """
        if step.name in ['none', 'all']:
            return step.name
        else:
            return step.method
        
    def _set_metrics(self, x, y):
        """
        Sets default metrics for the results table based upon the type of
        model used.
        """
        if self.algorithm_type == 'classifier':
            self.metrics_dict = {
                     'accuracy' : met.accuracy_score(y, self.predictions),
                     'balanced_accuracy' : met.balanced_accuracy_score(y,
                               self.predictions), 
                     'brier_score_loss' : 1 - met.brier_score_loss(y, 
                               self.predictions),
                     'cohen_kappa' : met.cohen_kappa_score(y, 
                                                           self.predictions),
                     'f1' : met.f1_score(y, self.predictions),                                           
                     'f1_weighted' : met.f1_score(y, self.predictions,
                                                  average = 'weighted'), 
                     'fbeta' : met.fbeta_score(y, self.predictions, 1),
                     'hamming' : met.hamming_loss(y, self.predictions),
                     'jaccard' : met.jaccard_similarity_score(y, 
                                self.predictions),
                     'hinge_loss' : met.hinge_loss(y, self.predictions),
                     'matthews_corrcoef' : met.matthews_corrcoef(y, 
                                self.predictions),
                     'neg_log_loss' :  1 - met.log_loss(y, self.predictions),                                                                 
                     'precision' :  met.precision_score(y, self.predictions),
                     'precision_weighted' :  met.precision_score(y, 
                                self.predictions, average = 'weighted'),
                     'recall' :  met.recall_score(y, self.predictions),
                     'recall_weighted' :  met.recall_score(y, 
                                                         self.predictions,
                                                         average = 'weighted'),
                     'roc_auc' :  met.roc_auc_score(y, self.predictions),
                     'zero_one' : met.zero_one_loss(y, self.predictions)}
        elif self.algorithm_type == 'regressor':
            self.metrics_dict = {}
        elif self.algorithm_type == 'grouper':
            self.metrics_dict = {}
        return self
   
    def add_metric(self, name, metric):
        """
        Allows user to manually add a metric to the results table.
        """
        self.metric_dict.update({name : metric})
        return self
        
    def add_result(self, tube, use_val_set = False): 
        """
        Adds the results of a single tube application to the results table.
        """              
        self.predictions = tube.model.method.predict(tube.data.x_test)
        self.pred_probs = tube.model.method.predict_proba(tube.data.x_test)
        self._set_metrics(tube.data.x_test, tube.data.y_test)
        new_row = pd.Series(index = self.columns)
        new_row['tube_number'] = tube.tube_num
        new_row['predictors'] = self._check_none(tube.splicer)
        new_row['scaler'] = self._check_none(tube.scaler)
        new_row['splitter'] = self._check_none(tube.splitter)
        new_row['encoder'] = self._check_none(tube.encoder)
        new_row['interactor'] = self._check_none(tube.interactor)
        new_row['splicer'] = self._check_none(tube.splicer)
        new_row['sampler'] = self._check_none(tube.sampler)
        new_row['selector'] = self._check_none(tube.selector)
        new_row['estimator'] = self._check_none(tube.model)
        new_row['seed'] = tube.model.seed
        new_row['validation_set'] = use_val_set
        for key, value in self.metrics_dict.items():
            if key in self.settings['metrics']:
                new_row[key] = value
        self.table.loc[len(self.table)] = new_row
        self._other_results(tube)
        return self
        
    def _other_results(self, tube):
        """
        Creates attributes storing other common metrics and tables.
        """
        self.confusion = met.confusion_matrix(tube.data.y_test, 
                                              self.predictions)
        self.class_report = met.classification_report(tube.data.y_test, 
                                                      self.predictions)
        self.feature_list = list(tube.data.x_test.columns)
        self.feature_import = pd.Series(
                data = tube.model.method.feature_importances_,
                index = self.feature_list)
        self.feature_import.sort_values(ascending = False, 
                                        inplace = True)
        if self.verbose:
            print('These are the results using the', tube.model.name, 
                  'model')
            print('Testing', tube.splicer.name, 'predictors')
            print('Confusion Matrix:')
            print(self.confusion)
            print('Classification Report:')
            print(self.class_report)
        return self