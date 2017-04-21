from sklearn.pipeline import Pipeline
# from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import GridSearchCV
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.externals import joblib
import pandas as pd
from sklearn import cross_validation, metrics
# pipeline = Pipeline([


# ('clf',GradientBoostingRegressor( init='None',loss='deviance')),
# ]);
# parameters={
#   'n_estimators': 100,
#   'learning_rate': 0.1,
#   'subsample':[0.7,0.75,0.8]
#
#
# }
gbdt=GradientBoostingClassifier(
  # loss='deviance'
#   learning_rate=0.1
# , n_estimators=100
# , subsample=0.8
# , min_samples_split=2
# , min_samples_leaf=1
# , max_depth=3
# , init=None
# , random_state=None
# , max_features=None
# , alpha=0.9
# , verbose=0
# , max_leaf_nodes=None
# , warm_start=False
)
# train_feat=pd.read_
train_feat=np.genfromtxt("/home/xuyonglong/feature/working/experiment/20170320/algo/train_r",dtype=np.float32)
train_id=np.genfromtxt("/home/xuyonglong/feature/working/experiment/20170320/algo/train_y",dtype=np.float32)
test_feat=np.genfromtxt("/home/xuyonglong/feature/working/experiment/20170320/algo/test_r",dtype=np.float32)
test_id=np.genfromtxt("/home/xuyonglong/feature/working/experiment/20170320/algo/test_y",dtype=np.float32)
# print train_feat.shape,train_id.shape,test_feat.shape,test_id.shape
gbdt.fit(train_feat,train_id)
pred=gbdt.predict(test_feat)
# print pred
# print pred[0]
# print len(pred)
joblib.dump(gbdt, 'gbdtmodel')
# y_predprob = gbdt.predict_proba(test_feat)[:,1]
# print y_predprob
# print y_predprob
# print "Accuracy : %.4g" % metrics.accuracy_score(test_id, pred)
# print "AUC Score (Train): %f" % metrics.roc_auc_score(test_id, y_predprob)
# grid = GridSearchCV(pipeline, cv=3, param_grid=parameters)
# grid.fit(train_feat, train_id)
