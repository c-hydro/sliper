"""
Library Features:

Name:          lib_utils_predictors
Author(s):     Stefania Magri (stefania.magri@arpal.liguria.it)
Date:          '20220105'
Version:       '1.0.0'
"""

#######################################################################################
# Library
import numpy as np
import numpy.linalg as la
#######################################################################################


# -------------------------------------------------------------------------------------
# Normalizing the data
def normalize(X):
    n, D = np.shape(X)
    Xmax = np.zeros((1, D))
    X_nor = np.zeros((n, D))

    for i in range(0, D):
        Xmax[:,i] = np.max(X[:,i])
    
    X_nor = (X / Xmax)
    return X_nor
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# We center the generated data 
def center(X):
    n, D = np.shape(X)
    XC = np.zeros((1, D))
    for i in range(0, D):
        XC[:,i] = np.mean(X[:,i])

    Xc = np.zeros((n, D))

    Xc = X - XC
    return Xc
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# The function computes the first k eigenvectors of the covariance matrix. 
def explainedVariance(eig_vals):
    
    tot = sum(eig_vals)
    var_exp = [(i / tot)*100 for i in eig_vals]
    cum_var_exp = np.cumsum(var_exp)
    
    return cum_var_exp
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# PCA function
def PCA(X, k):
   
    # Compute the covariance matrix of X
    cov_mat = np.cov(X.T)
    # compute eigenvalues and eigenvectors of the covariance matrix
    eigvals, eigvec = np.linalg.eig(cov_mat)
    
    # sort the eigenvalues in decreasing order and obtain the corresponding indexes  
    eigval_ord_asc = np.sort(eigvals)
    eigval_ord = eigval_ord_asc[::-1]
    eigval_idx_asc = np.argsort(eigvals)
    eigval_idx = eigval_idx_asc[::-1]
    
    # print('eigval_idx' , eigval_idx)
  
    # select the first k eigenvectors (Principal Components)
    PC = eigvec[:, eigval_idx[0:k]]
    # compute the Cumulative Explained Variance    
    expl_var = explainedVariance(eigval_ord[0:k])
    
    return PC, expl_var
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# To graph the projected points of PCA
def PCA_Project(X, PC):

    # standardize the data
    mean = X.mean(axis=0)
    X_z = X - mean
    # obtain the projected points
    X_proj = np.dot(X_z, PC)
    
    return X_proj
# -------------------------------------------------------------------------------------


############################################################
#FUNCTIONS FOR THE KERNEL REGRESSION

#The function sqDist computes all the distances between two sets of points stored in two matrices X1 and X2. 

def sqDist(X1, X2):
    sqx = np.sum(np.multiply(X1, X1), 1)
    sqy = np.sum(np.multiply(X2, X2), 1)

    return np.outer(sqx, np.ones(sqy.shape[0])) + np.outer(np.ones(sqx.shape[0]), sqy.T) - 2 * np.dot(X1, X2.T)

#####################################################
#Kernel Matrix
#The function KernelMatrix builds the matrix of the kernel (also called Gram matrix) between two sets of points stored in two matrices X1 and X2.
#param: is [] for the linear kernel, the exponent of the polynomial kernel, or the standard deviation for the gaussian kernel

def kernelMatrix(X1, X2, kernel_type, param):
   
    if kernel_type == 'linear':
        K = (np.dot(X1, X2.T))
        return K
    
    elif kernel_type == 'polynomial':
        K = (1 + np.dot(X1, X2.T)) ** param
        return K
    
    elif kernel_type == 'gaussian':
        dist = sqDist(X1, X2)
        K0 = dist **2
        s = 2*(param **2)
        K = np.exp(-1.0 * K0 / s)
        return  K

######################
#The function regularizedKernLSTrain computes the parameters of the function estimated on the training set.
##### c = regularizedKernLSTrain(Xtr, Ytr, kernel, sigma, lam)
#where
#- Xtr: training input
#- Ytr: training output
#- kernel_type: type of kernel ('linear', 'polynomial', 'gaussian')
#- param: is [] for the linear kernel, the exponent of the polynomial kernel, or the standard deviation for the gaussian kernel
#- reg_par: regularization parameter
#- c: model coefficients

def regularizedKernLSTrain(Xtr, Ytr, kernel_type, param, reg_par):
   
    K = kernelMatrix(Xtr, Xtr, kernel_type, param)

    nK = K.shape[0]
    DK = K.shape[1]
    I = np.identity(DK)
    
     #verifico che la matrice K sia quadrata
    if  nK != DK:
        print ("ATTENZIONE La matrice K NON e' quadrata")
    else:    
        A = K + reg_par * nK * I
    
        #verifico che la matrice sia invertibile
        Det = np.linalg.det(A)
        #if Det == 0:
            #print ("La matrice non e' invertibile")
        
        #verifico che la matrice sia definita positiva 
        DP = np.all(np.linalg.eigvals(A) > 0)
        if DP != False:

            L = np.linalg.cholesky(A)
            y = np.linalg.solve(L, Ytr)
            LT = np.transpose(L)
    
            c = np.linalg.solve(LT, y)
    
        else:        
            # Calcolo la matrice inversa con la eigendecomposition  
            eigvalsK, eigvectsK = np.linalg.eig(K)
            tt = np.isreal(np.linalg.eigvals(K))
            if np.all(tt) == True:
    
                V = eigvectsK;
                VT = np.transpose(V)
                S = eigvalsK;
                SI = S + reg_par * nK * I
                SIinv = np.linalg.inv(SI)
                B = np.dot(V, SIinv)
                BB= np.dot(B, VT)
        
                c = np.dot(BB,Ytr)

    return c

##################
#The function regularizedKernLSTest applies the estimated function to a test set, to verify its goodness. Use it as follows:
# Ypred = regularizedKernLSTest(c, Xtr, kernel_type, param, Xte)
# where
# - c: model coefficients
# - Xtr: training input
# - kernel type: type of kernel ('linear', 'polynomial', 'gaussian')
# - kernel type: type of kernel ('linear', 'polynomial', 'gaussian')
# - param: is [] for the linear kernel, the exponent of the polynomial kernel, or the standard deviation for the gaussian kernel
# - Xte: test points
# - Ypred: predicted output on the test set

def regularizedKernLSTest(c, Xtr, kernel_type, param, Xte):
   
    Ktest = kernelMatrix(Xte, Xtr, kernel_type, param)
    Ypred = np.dot(Ktest, c)
    Ypred = Ypred.astype(float)
    Ypred = np.round(Ypred)
    return Ypred

###########################################
#The function calcErr computes the error between real and predicted output (for regression problems).
# error is estimated as Mean Squared Error (MSE)

def calcErr(Ypred, Ytrue):
    MSE = np.mean((Ypred-Ytrue)**2)
    return MSE


##################################
#VFold Cross Validation

def VFoldCVKernRLS(x, y, KF, kernel_type, lam_list, kerpar_list):
   
    if KF <= 0:
        print("Please supply a positive number of repetitions")
        return -1

    if isinstance(kerpar_list, int):
        kerpar_list = np.array([kerpar_list])
    else:
        kerpar_list = np.array(kerpar_list)
    nkerpar = kerpar_list.size

    if isinstance(lam_list, int):
        lam_list = np.array([lam_list])
    else:
        lam_list = np.array(lam_list)
    nlambda = lam_list.size

    n = x.shape[0]
    n_val = int(np.ceil(n/KF))
    ntr = n - n_val

    tm = np.zeros((nlambda, nkerpar))
    ts = np.zeros((nlambda, nkerpar))
    vm = np.zeros((nlambda, nkerpar))
    vs = np.zeros((nlambda, nkerpar))

    ym = float(y.max() + y.min()) / float(2)
    
     # Random permutation of training data
    rand_idx = np.random.choice(n, size=n, replace=False)

    il = 0
    for l in lam_list:
        iss = 0
        for s in kerpar_list:
            trerr = np.zeros((KF, 1))
            vlerr = np.zeros((KF, 1))
            first=0
            for fold in range(KF):
                
                flags = np.zeros(x.shape[0])
                flags[first:first+n_val]=1;
            
                X = x[rand_idx[flags==0]]
                Y = y[rand_idx[flags==0]]
                X_val = x[rand_idx[flags==1]]
                Y_val = y[rand_idx[flags==1]]

                c = regularizedKernLSTrain(X, Y, kernel_type, s, l)
                
                trerr[fold] = calcErr(regularizedKernLSTest(c, X, kernel_type, s, X), Y)
                vlerr[fold] = calcErr(regularizedKernLSTest(c, X, kernel_type, s, X_val), Y_val)
                
            tm[il, iss] = np.median(trerr)
            ts[il, iss] = np.std(trerr)
            vm[il, iss] = np.median(vlerr)
            vs[il, iss] = np.std(vlerr)
            iss = iss + 1
        il = il + 1
    row, col = np.where(vm == np.amin(vm))
    l = lam_list[row]
    s = kerpar_list[col]
    
    return [l, s, vm, vs, tm, ts]

##################################################

def VFoldCVSVR(x, y, KF, kernel_type, lam_list, kerpar_list):
    
    from sklearn.svm import SVR
    
    if isinstance(kerpar_list, int):
        kerpar_list = np.array([kerpar_list])
    else:
        kerpar_list = np.array(kerpar_list)
    nkerpar = kerpar_list.size
   
    if isinstance(lam_list, int):
        lam_list = np.array([lam_list])
    else:
        lam_list = np.array(lam_list)
    nlambda = lam_list.size

    n = x.shape[0]
    n_val = int(np.ceil(n/KF))
    ntr = n - n_val
    
    print('training', ntr)
    print('validation', n_val)
    
       
    tm = np.zeros((nlambda, nkerpar))
    ts = np.zeros((nlambda, nkerpar))
    vm = np.zeros((nlambda, nkerpar))
    vs = np.zeros((nlambda, nkerpar))

    ym = float(y.max() + y.min()) / float(2)
    
    # Random permutation of training data
    rand_idx = np.random.choice(n, size=n, replace=False)

    il = 0
    for l in lam_list:
        iss = 0
        for s in kerpar_list:
            trerr = np.zeros((KF, 1))
            vlerr = np.zeros((KF, 1))
            first=0
            for fold in range(KF):
                
                flags = np.zeros(x.shape[0])
                flags[first:first+n_val]=1;
            
                X = x[rand_idx[flags==0]]
                Y = y[rand_idx[flags==0]]
                X_val = x[rand_idx[flags==1]]
                Y_val = y[rand_idx[flags==1]]
                
                #Yt = np.reshape(Y, (Y.shape[0],1)
                             
                regression_poly = SVR(kernel = kernel_type, C=l, gamma='scale', epsilon=0.1, degree = s)
                regression_poly.fit(X, Y)
                Ypredt = regression_poly.predict(X)
                Ypredt = np.round(Ypredt)
                trerr[fold] = uti.calcErr(Ypredt, Y)            
                
                Ypred_v = regression_poly.predict(X_val)
                Ypred_v = np.round(Ypred_v)
                vlerr[fold] = uti.calcErr(Ypred_v, Y_val)
                                                                           
            tm[il, iss] = np.median(trerr)
            ts[il, iss] = np.std(trerr)
            vm[il, iss] = np.median(vlerr)
            vs[il, iss] = np.std(vlerr)
            iss = iss + 1
        il = il + 1
    row, col = np.where(vm == np.amin(vm))
    
    l = lam_list[row]
    s = kerpar_list[col]
    
    return [l, s, vm, vs, tm, ts]
##################################################

def VFoldCVRBF(x, y, KF, kernel_type, lam_list, kerpar_list):
    
    #from sklearn.svm import SVR
    
    if isinstance(kerpar_list, int):
        kerpar_list = np.array([kerpar_list])
    else:
        kerpar_list = np.array(kerpar_list)
    nkerpar = kerpar_list.size
   
    if isinstance(lam_list, int):
        lam_list = np.array([lam_list])
    else:
        lam_list = np.array(lam_list)
    nlambda = lam_list.size

    n = x.shape[0]
    n_val = int(np.ceil(n/KF))
    ntr = n - n_val
    
    print('training', ntr)
    print('validation', n_val)
    
       
    tm = np.zeros((nlambda, nkerpar))
    ts = np.zeros((nlambda, nkerpar))
    vm = np.zeros((nlambda, nkerpar))
    vs = np.zeros((nlambda, nkerpar))

    ym = float(y.max() + y.min()) / float(2)
    
    # Random permutation of training data
    rand_idx = np.random.choice(n, size=n, replace=False)

    il = 0
    for l in lam_list:
        iss = 0
        for s in kerpar_list:
            trerr = np.zeros((KF, 1))
            vlerr = np.zeros((KF, 1))
            first=0
            for fold in range(KF):
                
                flags = np.zeros(x.shape[0])
                flags[first:first+n_val]=1;
            
                X = x[rand_idx[flags==0]]
                Y = y[rand_idx[flags==0]]
                X_val = x[rand_idx[flags==1]]
                Y_val = y[rand_idx[flags==1]]
                
                #Yt = np.reshape(Y, (Y.shape[0],1)
                             
                regression_rbf = SVR(kernel='rbf', C=l, gamma='scale')
                regression_rbf.fit(X, Y)
                Ypredt = regression_rbf.predict(X)
                Ypredt = np.round(Ypredt)
                trerr[fold] = uti.calcErr(Ypredt, Y)            
                
                Ypred_v = regression_rbf.predict(X_val)
                Ypred_v = np.round(Ypred_v)
                vlerr[fold] = uti.calcErr(Ypred_v, Y_val)
                                                                           
            tm[il, iss] = np.median(trerr)
            ts[il, iss] = np.std(trerr)
            vm[il, iss] = np.median(vlerr)
            vs[il, iss] = np.std(vlerr)
            iss = iss + 1
        il = il + 1
    row, col = np.where(vm == np.amin(vm))
    
    l = lam_list[row]
    s = kerpar_list[col]
    
    return [l, s, vm, vs, tm, ts]

##################################################
#identifico l'allerta

def Allerta(Ypred):
    nTe = np.shape(Ypred)
    Allerta = np.zeros((nTe,1))

    for i in range(0, nTe):
        if test_data['frane_pred'][i] > 13:
            Allerta[i] = 4
        elif test_data['frane_pred'][i] > 5:
            Allerta[i] = 3
        elif test_data['frane_pred'][i] > 2:
            Allerta[i] = 2
        elif test_data['frane_pred'][i] >= 1:
            Allerta[i] = 1
        else:
            Allerta[i] = 0

    return Allerta

##################################################
