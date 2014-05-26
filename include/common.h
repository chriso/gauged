/*!
 * Gauged
 * https://github.com/chriso/gauged (MIT Licensed)
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#ifndef GAUGED_COMMON_H_
#define GAUGED_COMMON_H_

#define GAUGED_OK    1
#define GAUGED_ERROR 0

#define ZTMP(i, line) ZTMP_(i, line)
#define ZTMP_(a, b) a##b

#if defined(WIN32) || defined(_WIN32)
# define GAUGED_EXPORT __declspec(dllexport)
#else
# define GAUGED_EXPORT
#endif

#endif
