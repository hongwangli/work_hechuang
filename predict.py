# -*- cocoding: utf-8 -*-
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.externals import joblib
test_feat=np.genfromtxt("/home/xuyonglong/feature/working/experiment/20170320/algo/test_r",dtype=np.float32)
test_id=np.genfromtxt("/home/xuyonglong/feature/working/experiment/20170320/algo/test_y",dtype=np.float32)
# joblib.dump(gbdt, 'gbdtmodel')
gbdt=joblib.load('gbdtmodel')
# pred=gbdt.predict(test_feat)
# print pred



# y_predprob = gbdt.predict_proba(test_feat)[:,1 ]
# print type(y_predprob)
# print "Accuracy : %.4g" % metrics.accuracy_score(y.values, y_pred)
# print "AUC Score (Train): %f" % metrics.roc_auc_score(y, y_predprob)


print gbdt.apply(test_feat)